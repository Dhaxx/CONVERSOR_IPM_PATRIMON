import psycopg2 as pg
import psycopg2.extras 
import fdb

conexao_origem = pg.connect(
    host="localhost",
    port="5432",
    database="piracaia",
    user="postgres",
    password="Dnal250304"    
)

conexao_destino = fdb.connect(dsn="localhost:D:\Fiorilli\SCPI_8\Cidades\PIRACAIA-PM\ARQ2022\SCPI2022.FDB", user='FSCSCPI8', 
                              password='scpi', port=3050, charset='win1252')

conexao_aux = fdb.connect(dsn="localhost:D:\Fiorilli\SCPI_8\Cidades\PIRACAIA-PM\ARQ2022\SCPI2022-old.FDB", user='FSCSCPI8', 
                              password='scpi', port=3050, charset='UTF8')

cur = conexao_origem.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
cur_d = conexao_destino.cursor()
cur_a = conexao_aux.cursor()

def commit():
    conexao_destino.commit()
    print("Commited")

def get_cursor(conexao):
    return conexao.cursor()