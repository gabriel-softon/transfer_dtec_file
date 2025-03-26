import os
from datetime import datetime
import logging
import subprocess
import sys

from index import fetch_registros

CAT_PREFIX = {
    'LD': 'N',
    'CR': 'C',
    'FF': 'N',
    'SE': 'E',
    'SA': 'A'
}

DATE_DIRECTORY = datetime.now().strftime("%Y%m%d")

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
logger.info("Iniciando o script de teste de log.")

# PATH_BASE = '/media/noticias_www'
LOCAL_PATH_BASE = '/home/softon/test3'
REMOTE_PATH_BASE = '/home/ubuntu/test3'

for chave, valor in CAT_PREFIX.items():
    local_dir = f"{LOCAL_PATH_BASE}/{chave}/{valor}{DATE_DIRECTORY}"
    remote_dir = f"{REMOTE_PATH_BASE}/{chave}/{valor}{DATE_DIRECTORY}"
    rsync_command = (
        f'rsync -az --rsync-path="mkdir -p {remote_dir} && rsync" '
        f'-e "ssh -i /home/softon/keypairs/rsa_key_file_3072 -p 8022" '
        f'"{local_dir}" "ubuntu@dtec-flex.com.br:{remote_dir}"'
    )
    result = subprocess.run(rsync_command, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        logger.info(f"Transferência concluída com sucesso para: {local_dir}")
    else:
        logger.error(f"Erro na transferência de {local_dir}: {result.stderr}")

registros = fetch_registros()
missing = []

for chave, valor in CAT_PREFIX.items():
    # remote_dir = f'/mnt/dtecflex-site-root/{chave}/{valor}{DATE_DIRECTORY}'
    remote_dir = f'/mnt/dtecflex-site-root/{chave}/{valor}{DATE_DIRECTORY}'
    logger.info(f"Verificando diretório remoto: {remote_dir}")
    ssh_command = (
        f'ssh -i /home/softon/keypairs/rsa_key_file_3072 -p 8022 '
        f'ubuntu@dtec-flex.com.br "ls -l {remote_dir}"'
    )
    result = subprocess.run(ssh_command, shell=True, capture_output=True, text=True)
    
    logger.info(f"Conteúdo de {remote_dir}:\n{result.stdout}")
    
    for reg in registros:
        if reg.get('cat_abrev') == chave:
            if f"{reg['REG_NOTICIA']}_arquivos" not in result.stdout:
                missing.append(reg)
                logger.warning(f"Notícia {reg['REG_NOTICIA']} não encontrada em {remote_dir}")

if missing:
    logger.info("As seguintes REG_NOTICIA não foram encontradas nos diretórios remotos:")
    for reg in missing:
        logger.info(reg['REG_NOTICIA'])
else:
    logger.info("Todos os REG_NOTICIA foram encontrados nos diretórios remotos!")
