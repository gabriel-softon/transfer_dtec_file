import datetime
import os
import subprocess
import logging
import glob
from datetime import datetime
from collections import defaultdict
import sys
import mysql.connector
from dotenv import load_dotenv

# DATE_DIRECTORY = '20250624'
DATE_DIRECTORY = datetime.now().strftime("%Y%m%d")
PATH_BASE_REMOTE = '/mnt/dtecflex-site-root'
# PATH_BASE_REMOTE = '/mnt/dtecflex-site-root/test5'

load_dotenv()

log_dir = "./log"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = os.path.join(log_dir, f"log_{DATE_DIRECTORY}.txt")

def get_logger():
    logger = logging.getLogger("transfer_logger")
    logger.setLevel(logging.INFO)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    file_handler = logging.FileHandler(log_filename, mode="a")
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    return logger

logger = get_logger()

# PATH_BASE = '/home/softon/test3'
PATH_BASE = '/media/noticias_www'

CAT_ABREV = {
    'Lavagem de Dinheiro': 'LD',
    'Crime':               'CR',
    'Fraude':              'FF',
    'Empresarial':         'SE',
    'Ambiental':           'SA'
}

CAT_PREFIX = {
    'LD': 'N',
    'CR': 'C',
    'FF': 'N',
    'SE': 'E',
    'SA': 'A'
}

CATEGORY_MAPPING = {
    'Lavagem de Dinheiro': ('Lavagem de Dinheiro', 'DTECFLEX'),
    'Crime':               ('Crimes',             'DTECCRIM'),
    'Fraude':              ('Fraude Financeira',  'DTECFLEX'),
    'Empresarial':         ('Saúde Empresarial',  'DTECEMP'),
    'Ambiental':           ('SocioAmbiental',     'DTECAMB'),
}

DB_CONFIG = {
    'user':     'dtecflex',
    'password': 'softon1245',
    'host':     'dtec-flex.com.br',
    # 'host':     '10.10.10.24',
    'port':     '3306',
    'database': 'dtecflex'
}

def fetch_registros():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT *
            FROM TB_NOTICIA_RASPADA
            WHERE STATUS = '201-APPROVED'
        """
        cursor.execute(query)
        registros = cursor.fetchall()

        for reg in registros:
            reg['CAT_ABREV']  = CAT_ABREV.get(reg['CATEGORIA'])
            reg['CAT_PREFIX']= CAT_PREFIX.get(reg['CAT_ABREV'])

        return registros
    except mysql.connector.Error as err:
        logger.error(f"Erro no banco de dados: {err}")
        return []
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

def agrupar_registros(registros):
    grupos = defaultdict(list)
    for reg in registros:
        grupos[reg['CATEGORIA']].append(reg)
    return [{"CATEGORIA": cat, "REGISTROS": regs} for cat, regs in grupos.items()]

def construir_caminhos(registro):
    local_pattern = f"{PATH_BASE}/{registro['CAT_ABREV']}/{registro['CAT_PREFIX']}{DATE_DIRECTORY}/{registro['REG_NOTICIA']}*"
    remote_dir    = f"{PATH_BASE_REMOTE}/{registro['CAT_ABREV']}/{registro['CAT_PREFIX']}{DATE_DIRECTORY}"
    return local_pattern, remote_dir

def transferir_arquivo(local_pattern, remote_dir, noticia_id):
    itens = glob.glob(local_pattern)
    if not itens:
        logger.warning(f"Nenhum item encontrado para o padrão: {local_pattern}")
        return False

    mkdir_command = (
        f'ssh -i /home/softon/keypairs/rsa_key_file_3072 -p 8022 '
        f'ubuntu@dtec-flex.com.br "mkdir -p {remote_dir}"'
    )
    mkdir_result = subprocess.run(mkdir_command, shell=True, capture_output=True, text=True)
    if mkdir_result.returncode != 0:
        logger.error(f"Erro ao criar diretório remoto {remote_dir}: {mkdir_result.stderr}")
        return False

    itens_str = " ".join(f'"{item}"' for item in itens)
    rsync_command = (
        f'rsync -az --no-perms --no-owner --no-group --no-times --omit-dir-times --size-only '
        f'-e "ssh -i /home/softon/keypairs/rsa_key_file_3072 -p 8022" '
        f'{itens_str} ubuntu@dtec-flex.com.br:{remote_dir}'
    )
    logger.info(f"Executando rsync: {rsync_command}")
    result = subprocess.run(rsync_command, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        logger.info(f"Transferência concluída com sucesso para o padrão: {local_pattern}")
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            update_sql = "UPDATE TB_NOTICIA_RASPADA SET STATUS=%s, DT_TRANSFERENCIA=NOW() WHERE ID=%s"
            cursor.execute(update_sql, ("205-TRANSFERED", noticia_id))
            conn.commit()
            logger.info(f"{cursor.rowcount} registro(s) atualizado(s) para status 205-TRANSFERED")
        except mysql.connector.Error as err:
            logger.error(f"Erro ao atualizar TB_NOTICIA_RASPADA após transferência: {err}")
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn.is_connected():
                conn.close()
        return True
    else:
        logger.error(f"Erro na transferência para o padrão {local_pattern}: {result.stderr}")
        return False

def fetch_noticias_publicadas():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                r.ID              AS news_id,
                r.LINK_ID, r.URL, r.FONTE, r.DATA_PUBLICACAO,
                r.CATEGORIA, r.REG_NOTICIA, r.QUERY,
                r.ID_ORIGINAL, r.LINK_ORIGINAL,
                r.DT_RASPAGEM, r.DT_DECODE, r.TITULO,
                r.ID_USUARIO, r.STATUS,
                r.TENTATIVA_EXTRAIR, r.TEXTO_NOTICIA,
                r.REGIAO, r.UF, r.DT_APROVACAO,
                n.ID              AS name_id,
                n.NOME, n.CPF, n.NOME_CPF, n.APELIDO,
                n.SEXO, n.PESSOA, n.IDADE, n.ATIVIDADE,
                n.ENVOLVIMENTO, n.TIPO_SUSPEITA,
                n.FLG_PESSOA_PUBLICA, n.ANIVERSARIO,
                n.INDICADOR_PPE,
                n.OPERACAO        AS OPERACAO
            FROM TB_NOTICIA_RASPADA r
            LEFT JOIN TB_NOTICIA_RASPADA_NOME n 
            ON r.ID = n.NOTICIA_ID
            WHERE r.STATUS = '205-TRANSFERED'
            AND r.DT_TRANSFERENCIA >= CURRENT_DATE
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        noticias = {}
        for row in rows:
            nid = row['news_id']
            if nid not in noticias:
                noticias[nid] = {
                    'ID':              nid,
                    'LINK_ID':         row.get('LINK_ID'),
                    'URL':             row.get('URL'),
                    'FONTE':           row.get('FONTE'),
                    'DATA_PUBLICACAO': row.get('DATA_PUBLICACAO'),
                    'CATEGORIA':       row.get('CATEGORIA'),
                    'REG_NOTICIA':     row.get('REG_NOTICIA'),
                    'TEXTO_NOTICIA':   row.get('TEXTO_NOTICIA'),
                    'UF':              row.get('UF'),
                    'REGIAO':          row.get('REGIAO'),
                    'OPERACAO':        row.get('OPERACAO'),
                    'TITULO':          row.get('TITULO'),
                    'NAMES':           []
                }
            if row.get('name_id') is not None:
                noticias[nid]['NAMES'].append({
                    'NOME':               row.get('NOME'),
                    'CPF':                row.get('CPF'),
                    'NOME_CPF':           row.get('NOME_CPF'),
                    'APELIDO':            row.get('APELIDO'),
                    'SEXO':               row.get('SEXO'),
                    'PESSOA':             row.get('PESSOA'),
                    'OPERACAO':           row.get('OPERACAO'),
                    'IDADE':              row.get('IDADE'),
                    'ATIVIDADE':          row.get('ATIVIDADE'),
                    'ENVOLVIMENTO':       row.get('ENVOLVIMENTO'),
                    'FLG_PESSOA_PUBLICA': row.get('FLG_PESSOA_PUBLICA'),
                    'ANIVERSARIO':        row.get('ANIVERSARIO'),
                    'INDICADOR_PPE':      row.get('INDICADOR_PPE'),
                })
        return list(noticias.values())
    except mysql.connector.Error as err:
        logger.error(f"Erro no banco de dados: {err}")
        return []
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

def insert_names_to_aux(noticias):
    published_news     = []
    not_published_news = []
    total_inserted     = 0

    try:
        conn   = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO Auxiliar (
                NOME, CPF, NOME_CPF, APELIDO, DTEC,
                SEXO, PESSOA, IDADE, ATIVIDADE, ENVOLVIMENTO,
                TIPO_SUSPEITA, OPERACAO, TITULO, DATA_NOTICIA, FONTE_NOTICIA,
                REGIAO, ESTADO, REGISTRO_NOTICIA, FLG_PESSOA_PUBLICA, DATA_GRAVACAO,
                EXISTEM_PROCESSOS, ORIGEM_UF, TRIBUNAIS, LINKS_TRIBUNAIS, DATA_PESQUISA,
                TIPO_INFORMACAO, ANIVERSARIO, CITACOES_NA_MIDIA, INDICADOR_PPE, PEP_RELACIONADO,
                LINK_NOTICIA, DATA_ATUALIZACAO, ORGAO, EMPRESA_RELACIONADA, CNPJ_EMPRESA_RELACIONADA,
                RELACIONAMENTO, DATA_INICIO_MANDATO, DATA_FIM_MANDATO, DATA_CARENCIA
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, NOW(),
                %s, %s, %s, %s, NOW(),
                %s, %s, %s, %s, %s,
                %s, NOW(), %s, %s, %s,
                %s, %s, %s, %s
            )
        """

        # campos fixos (None) para colunas estáticas
        dtec                      = None
        existem_processos         = None
        origem_uf                 = None
        tribunais                 = None
        links_tribunais           = None
        pep_relacionado           = None
        orgao                     = None
        empresa_relacionada       = None
        cnpj_empresa_relacionada  = None
        relacionamento            = None
        data_inicio_mandato       = None
        data_fim_mandato          = None
        data_carencia             = None

        for news in noticias:
            # aplicando o mapping conforme a categoria
            categoria            = news.get("CATEGORIA")
            tipo_suspeita, tipo_informacao = CATEGORY_MAPPING.get(categoria, (None, None))

            news_id    = news.get("ID")
            titulo     = news.get("TITULO")
            data_noticia = news.get("DATA_PUBLICACAO")
            fonte      = news.get("FONTE")
            regiao     = news.get("REGIAO")
            estado     = news.get("UF")
            reg_noticia = news.get("REG_NOTICIA")
            texto      = news.get("TEXTO_NOTICIA")

            names_inserted = 0
            for name in news.get("NAMES", []):
                values = (
                    name['NOME'],
                    name['CPF'],
                    name['NOME_CPF'],
                    name['APELIDO'],
                    dtec,
                    name['SEXO'],
                    name['PESSOA'],
                    name['IDADE'],
                    name['ATIVIDADE'],
                    name['ENVOLVIMENTO'],
                    tipo_suspeita,
                    name['OPERACAO'],
                    titulo,
                    data_noticia,
                    fonte,
                    regiao,
                    estado,
                    reg_noticia,
                    name['FLG_PESSOA_PUBLICA'],
                    existem_processos,
                    origem_uf,
                    tribunais,
                    links_tribunais,
                    tipo_informacao,
                    name['ANIVERSARIO'],
                    texto,
                    name['INDICADOR_PPE'],
                    pep_relacionado,
                    news.get("URL"),
                    orgao,
                    empresa_relacionada,
                    cnpj_empresa_relacionada,
                    relacionamento,
                    data_inicio_mandato,
                    data_fim_mandato,
                    data_carencia
                )
                try:
                    cursor.execute(insert_query, values)
                    total_inserted += 1
                    names_inserted += 1
                    logger.info(f"Nome '{name['NOME']}' inserido para notícia {news_id}")
                except Exception as err:
                    logger.error(f"Erro ao inserir '{name['NOME']}' (notícia {news_id}): {err}")

            if names_inserted > 0:
                try:
                    cursor.execute(
                        "UPDATE TB_NOTICIA_RASPADA SET STATUS=%s, DT_TRANSFERENCIA=NOW() WHERE ID=%s",
                        ("203-PUBLISHED", news_id)
                    )
                    published_news.append(news_id)
                except Exception as err:
                    logger.error(f"Erro ao atualizar notícia {news_id} para 203-PUBLISHED: {err}")
                    not_published_news.append(news_id)
            else:
                not_published_news.append(news_id)

        conn.commit()
        logger.info(f"Total de nomes inseridos na Auxiliar: {total_inserted}")

    except Exception as err:
        logger.error(f"Erro ao inserir na Auxiliar: {err}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

    return published_news, not_published_news

def main():
    registros = fetch_registros()
    if not registros:
        logger.info("Nenhum registro encontrado para a data atual.")
        return
    
    print('len?:::::', len(registros))

    grupos = agrupar_registros(registros)
    transfer_success = []
    transfer_fail    = []

    for grupo in grupos:
        logger.info(f"Categoria: {grupo['CATEGORIA']} - {len(grupo['REGISTROS'])} registros")
        for reg in grupo['REGISTROS']:
            lp, rd = construir_caminhos(reg)
            logger.info(f"Transferindo {lp} → {rd}")
            if transferir_arquivo(lp, rd, reg['ID']):
                transfer_success.append(reg['REG_NOTICIA'])
            else:
                transfer_fail.append(reg['REG_NOTICIA'])

    noticias = fetch_noticias_publicadas()
    published_news, not_published_news = insert_names_to_aux(noticias)

    final_report = f"""
Relatório Final:
  Notícias Publicadas:       {published_news}    (Total: {len(published_news)})
  Notícias Não Publicadas:   {not_published_news} (Total: {len(not_published_news)})
"""
    logger.info(final_report)

if __name__ == "__main__":
    main()
