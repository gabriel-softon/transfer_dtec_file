import os
import subprocess
import logging
from collections import defaultdict
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PATH_BASE = '/media/noticias_www'
DATE_DIRECTORY = datetime.now().strftime("%Y%m%d")

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
            WHERE DATE(DT_RASPAGEM) = CURDATE()
              AND STATUS = '10-URL-OK'
        """
        cursor.execute(query)
        registros = cursor.fetchall()
        
        for reg in registros:
            reg['cat_abrev'] = CAT_ABREV.get(reg['categoria'])
            reg['cat_prefix'] = CAT_PREFIX.get(reg['cat_abrev'])
        
        return registros
    except mysql.connector.Error as err:
        logging.error(f"Erro no banco de dados: {err}")
        return []
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

def agrupar_registros(registros):
    grupos = defaultdict(list)
    for reg in registros:
        grupos[reg['categoria']].append(reg)
    return [{"categoria": cat, "registros": regs} for cat, regs in grupos.items()]

def construir_caminhos(registro):
    local_dir = os.path.join(PATH_BASE, registro['cat_abrev'], f"{registro['cat_prefix']}{DATE_DIRECTORY}", registro['REG_NOTICIA'])
    remote_dir = f"/destino/{registro['cat_abrev']}/{registro['cat_prefix']}{DATE_DIRECTORY}/{registro['REG_NOTICIA']}"
    return local_dir, remote_dir

def transferir_arquivo(local_dir, remote_dir):
    if not os.path.exists(local_dir):
        logging.warning(f"Diretório local não encontrado: {local_dir}")
        return False

    rsync_command = (
        f'rsync -r -e "ssh -i /home/softon/keypairs/rsa_key_file_3072 -p 8022" '
        f'"{local_dir}" "ubuntu@dtec-flex.com.br:{remote_dir}"'
    )
    logging.info(f"Executando: {rsync_command}")
    result = subprocess.run(rsync_command, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        logging.info(f"Transferência concluída com sucesso para: {local_dir}")
        return True
    else:
        logging.error(f"Erro na transferência de {local_dir}: {result.stderr}")
        return False

def main():
    registros = fetch_registros()
    if not registros:
        logging.info("Nenhum registro encontrado para a data atual.")
        return
    
    for reg in registros:
        logging.info(
            f"Registro: REG_NOTICIA = {reg['REG_NOTICIA']}, "
            f"categoria = {reg['categoria']}, "
            f"cat_abrev = {reg['cat_abrev']}, "
            f"cat_prefix = {reg['cat_prefix']}"
        )
    
    grupos = agrupar_registros(registros)
    
    for grupo in grupos:
        logging.info(f"Categoria: {grupo['categoria']} - Total de registros: {len(grupo['registros'])}")
    
    for grupo in grupos:
        for reg in grupo["registros"]:
            local_dir, remote_dir = construir_caminhos(reg)
            logging.info(f"Preparando transferência do diretório: {local_dir}")
            # transferir_arquivo(local_dir, remote_dir)

if __name__ == "__main__":
    main()
