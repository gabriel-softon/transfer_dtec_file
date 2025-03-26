import os
import subprocess
import logging
import glob
from collections import defaultdict
import sys
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

# DATE_DIRECTORY = datetime.now().strftime("%Y%m%d")
DATE_DIRECTORY = '20250320'

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

PATH_BASE = '/media/noticias_www'

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

def transferir_arquivo(local_pattern, remote_dir):
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
        return True
    else:
        logger.error(f"Erro na transferência para o padrão {local_pattern}: {result.stderr}")
        return False

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
    
    for grupo in grupos:
        logger.info(f"Categoria: {grupo['CATEGORIA']} - Total de registros: {len(grupo['REGISTROS'])}")

    for grupo in grupos:
        for reg in grupo["REGISTROS"]:
            local_pattern, remote_dir = construir_caminhos(reg)
            logger.info(f"Preparando transferência para o padrão: {local_pattern}")
            transferir_arquivo(local_pattern, remote_dir)

if __name__ == "__main__":
    main()
