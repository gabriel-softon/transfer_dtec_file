import os
import subprocess
import logging
import glob
from collections import defaultdict
import sys
import mysql.connector
from dotenv import load_dotenv

DATE_DIRECTORY = '20250317'
# DATE_DIRECTORY = datetime.now().strftime("%Y%m%d")

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

PATH_BASE = '/home/softon/test3'
# PATH_BASE = '/media/noticias_www'

CAT_ABREV = {
    'Lavagem de Dinheiro': 'LD',
    'Crime': 'CR',
    'Fraude': 'FF',
    'Empresarial': 'SE',
    'Ambiental': 'SA'
}

CAT_PREFIX = {
    'LD': 'N',
    'CR': 'C',
    'FF': 'N',
    'SE': 'E',
    'SA': 'A'
}

DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_NAME')
}

def fetch_registros():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT *
            FROM TB_NOTICIA_RASPADA
            WHERE STATUS = '201-APPROVED'
                AND DT_APROVACAO >= current_date
        """
        cursor.execute(query)
        registros = cursor.fetchall()

        for reg in registros:
            reg['CAT_ABREV'] = CAT_ABREV.get(reg['CATEGORIA'])
            reg['CAT_PREFIX'] = CAT_PREFIX.get(reg['CAT_ABREV'])

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
    
    if registro['REG_NOTICIA'] == f"{registro['CAT_PREFIX']}{DATE_DIRECTORY}":
        remote_dir = f"/home/ubuntu/test3/{registro['CAT_ABREV']}/{registro['CAT_PREFIX']}{DATE_DIRECTORY}"
    else:
        remote_dir = f"/home/ubuntu/test3/{registro['CAT_ABREV']}/{registro['CAT_PREFIX']}{DATE_DIRECTORY}/{registro['REG_NOTICIA']}"
    
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
        f'rsync -az -e "ssh -i /home/softon/keypairs/rsa_key_file_3072 -p 8022" '
        f'{itens_str} "ubuntu@dtec-flex.com.br:{remote_dir}"'
    )
    logger.info(f"Executando rsync: {rsync_command}")
    result = subprocess.run(rsync_command, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        logger.info(f"Transferência concluída com sucesso para o padrão: {local_pattern}")
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)
            query = "UPDATE TB_NOTICIA_RASPADA SET STATUS = %s, DT_TRANSFERENCIA = NOW() WHERE ID = %s"
            cursor.execute(query, ("205-TRANSFERED", noticia_id))
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
                r.ID AS news_id,
                r.LINK_ID,
                r.URL,
                r.FONTE,
                r.DATA_PUBLICACAO,
                r.CATEGORIA,
                r.REG_NOTICIA,
                r.QUERY,
                r.ID_ORIGINAL,
                r.LINK_ORIGINAL,
                r.DT_RASPAGEM,
                r.DT_DECODE,
                r.TITULO,
                r.ID_USUARIO,
                r.STATUS,
                r.OPERACAO,
                r.TENTATIVA_EXTRAIR,
                r.TEXTO_NOTICIA,
                r.REGIAO,
                r.UF,
                r.DT_APROVACAO,
                n.ID AS name_id,
                n.NOME,
                n.CPF,
                n.NOME_CPF,
                n.APELIDO,
                n.SEXO,
                n.PESSOA,
                n.IDADE,
                n.ATIVIDADE,
                n.ENVOLVIMENTO,
                n.TIPO_SUSPEITA,
                n.FLG_PESSOA_PUBLICA,
                n.ANIVERSARIO,
                n.INDICADOR_PPE
            FROM TB_NOTICIA_RASPADA r
            LEFT JOIN TB_NOTICIA_RASPADA_NOME n ON r.ID = n.NOTICIA_ID
            WHERE r.STATUS = '205-TRANSFERED'
            AND DT_TRANSFERENCIA >= current_date
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        logger.info(f"DEBUG: Total de linhas retornadas: {len(rows)}")
        
        noticias = {}
        for row in rows:
            news_id = row['news_id']
            if news_id not in noticias:
                noticias[news_id] = {
                    'ID': row['news_id'],
                    'LINK_ID': row.get('LINK_ID'),
                    'URL': row.get('URL'),
                    'FONTE': row.get('FONTE'),
                    'DATA_PUBLICACAO': row.get('DATA_PUBLICACAO'),
                    'CATEGORIA': row.get('CATEGORIA'),
                    'REG_NOTICIA': row.get('REG_NOTICIA'),
                    'QUERY': row.get('QUERY'),
                    'ID_ORIGINAL': row.get('ID_ORIGINAL'),
                    'LINK_ORIGINAL': row.get('LINK_ORIGINAL'),
                    'DT_RASPAGEM': row.get('DT_RASPAGEM'),
                    'DT_DECODE': row.get('DT_DECODE'),
                    'TITULO': row.get('TITULO'),
                    'ID_USUARIO': row.get('ID_USUARIO'),
                    'STATUS': row.get('STATUS'),
                    'OPERACAO': row.get('OPERACAO'),
                    'TENTATIVA_EXTRAIR': row.get('TENTATIVA_EXTRAIR'),
                    'TEXTO_NOTICIA': row.get('TEXTO_NOTICIA'),
                    'REGIAO': row.get('REGIAO'),
                    'UF': row.get('UF'),
                    'DT_APROVACAO': row.get('DT_APROVACAO'),
                    'NAMES': []
                }
            if row.get('name_id') is not None:
                name_data = {
                    'ID': row.get('name_id'),
                    'NOME': row.get('NOME'),
                    'CPF': row.get('CPF'),
                    'NOME_CPF': row.get('NOME_CPF'),
                    'APELIDO': row.get('APELIDO'),
                    'SEXO': row.get('SEXO'),
                    'PESSOA': row.get('PESSOA'),
                    'IDADE': row.get('IDADE'),
                    'ATIVIDADE': row.get('ATIVIDADE'),
                    'ENVOLVIMENTO': row.get('ENVOLVIMENTO'),
                    'TIPO_SUSPEITA': row.get('TIPO_SUSPEITA'),
                    'FLG_PESSOA_PUBLICA': row.get('FLG_PESSOA_PUBLICA'),
                    'ANIVERSARIO': row.get('ANIVERSARIO'),
                    'INDICADOR_PPE': row.get('INDICADOR_PPE')
                }
                noticias[news_id]['NAMES'].append(name_data)
        
        logger.info(f"DEBUG: Total de notícias agregadas: {len(noticias)}")
        for news_id, news in noticias.items():
            logger.info(f"DEBUG: Notícia ID {news_id} possui {len(news.get('NAMES', []))} registro(s) de nomes")
        
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
    published_news = []
    not_published_news = []
    total_inserted = 0
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
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
        dtec = None
        existem_processos = None
        origem_uf = None
        tribunais = None
        links_tribunais = None
        tipo_informacao = None
        citacoes_na_midia = None
        pep_relacionado = None
        orgao = None
        empresa_relacionada = None
        cnpj_empresa_relacionada = None
        relacionamento = None
        data_inicio_mandato = None
        data_fim_mandato = None
        data_carencia = None

        for news in noticias:
            news_id         = news.get("ID")
            titulo          = news.get("TITULO")
            data_noticia    = news.get("DATA_PUBLICACAO")
            fonte_noticia   = news.get("FONTE")
            regiao          = news.get("REGIAO")
            estado          = news.get("UF")
            registro_noticia= news.get("REG_NOTICIA")
            citacoes_na_midia= news.get("TEXTO_NOTICIA")
            link_noticia    = news.get("URL")
            operacao         = news.get("OPERACAO")
            logger.info(f"Iniciando inserção dos nomes para notícia ID {news_id}")

            names_inserted = 0
            for name in news.get("NAMES", []):
                nome             = name.get("NOME")
                cpf              = name.get("CPF")
                nome_cpf         = name.get("NOME_CPF")
                apelido          = name.get("APELIDO")
                sexo             = name.get("SEXO")
                pessoa           = name.get("PESSOA")
                idade            = name.get("IDADE")
                atividade        = name.get("ATIVIDADE")
                envolvimento     = name.get("ENVOLVIMENTO")
                tipo_suspeita    = name.get("TIPO_SUSPEITA")
                flg_pessoa_publica = name.get("FLG_PESSOA_PUBLICA")
                aniversario      = name.get("ANIVERSARIO")
                indicador_ppe    = name.get("INDICADOR_PPE")

                values = (
                    nome, cpf, nome_cpf, apelido, dtec, 
                    sexo, pessoa, idade, atividade, envolvimento, 
                    tipo_suspeita, operacao, titulo, data_noticia, fonte_noticia, 
                    regiao, estado, registro_noticia, flg_pessoa_publica, existem_processos, 
                    origem_uf, tribunais, links_tribunais, tipo_informacao, 
                    aniversario, citacoes_na_midia, indicador_ppe, pep_relacionado, link_noticia,
                    orgao, empresa_relacionada, cnpj_empresa_relacionada, relacionamento, data_inicio_mandato, 
                    data_fim_mandato, data_carencia
                )
                try:
                    cursor.execute(insert_query, values)
                    total_inserted += 1
                    names_inserted += 1
                    logger.info(f"Nome '{nome}' inserido com sucesso para notícia ID {news_id}")
                except Exception as err:
                    logger.error(f"ERRO: Falha ao inserir nome '{nome}' para notícia ID {news_id} - {err}")
            if names_inserted > 0:
                try:
                    update_query = "UPDATE TB_NOTICIA_RASPADA SET STATUS = %s, DT_TRANSFERENCIA = NOW() WHERE ID = %s"
                    cursor.execute(update_query, ("203-PUBLISHED", news_id))
                    logger.info(f"Notícia ID {news_id} atualizada para 203-PUBLISHED")
                    published_news.append(news_id)
                except Exception as err:
                    logger.error(f"Erro ao atualizar notícia ID {news_id} para 203-PUBLISHED - {err}")
                    not_published_news.append(news_id)
            else:
                logger.warning(f"Nenhum nome inserido para notícia ID {news_id}. Status não atualizado.")
                not_published_news.append(news_id)
        conn.commit()
        logger.info(f"Total de nomes inseridos na tabela Auxiliar: {total_inserted}")
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
    
    for reg in registros:
        logger.info(
            f"Registro: REG_NOTICIA = {reg['REG_NOTICIA']}, "
            f"categoria = {reg['CATEGORIA']}, "
            f"cat_abrev = {reg['CAT_ABREV']}, "
            f"cat_prefix = {reg['CAT_PREFIX']}"
        )
    
    grupos = agrupar_registros(registros)
    
    transfer_success = []
    transfer_fail = []
    for grupo in grupos:
        logger.info(f"Categoria: {grupo['CATEGORIA']} - Total de registros: {len(grupo['REGISTROS'])}")
        for reg in grupo["REGISTROS"]:
            local_pattern, remote_dir = construir_caminhos(reg)
            logger.info(f"Preparando transferência para o padrão: {local_pattern}")
            resultado = transferir_arquivo(local_pattern, remote_dir, reg['ID'])
            if resultado:
                transfer_success.append(reg['REG_NOTICIA'])
            else:
                transfer_fail.append(reg['REG_NOTICIA'])
    
    noticias = fetch_noticias_publicadas()
    published_news, not_published_news = insert_names_to_aux(noticias)
    
    final_report = f"""
    Relatório Final:
    --------------------
    Arquivos Transferidos: {transfer_success} (Total: {len(transfer_success)})
    Arquivos Não Transferidos: {transfer_fail} (Total: {len(transfer_fail)})
    Notícias Publicadas (atualizadas para 203-PUBLISHED): {published_news} (Total: {len(published_news)})
    Notícias Não Publicadas: {not_published_news} (Total: {len(not_published_news)})
    --------------------
    """
    logger.info(final_report)

if __name__ == "__main__":
    main()
