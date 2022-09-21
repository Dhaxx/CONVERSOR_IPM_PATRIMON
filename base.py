import conexao as cnx
import manutencao

cur_orig = cnx.cur
cur_dest = cnx.cur_d

empresa = cur_dest.execute("SELECT empresa from cadcli").fetchone()[0]

def cria_campos():
    print("Criando campos")
    manutencao.cria_campo("desfor","modulo")
    manutencao.cria_campo("desfor","id")
    manutencao.cria_campo("pt_cadpat","codant")
    manutencao.cria_campo("pt_cadpatd","codant")
    manutencao.cria_campo("pt_cadpatd","codant_1")
    manutencao.cria_campo("pt_cadpatd","codant_2")
    manutencao.cria_campo("pt_cadpats","codant")
    manutencao.cria_campo("pt_cadpats","codant_1")
    manutencao.cria_campo("pt_cadpats","codant_2")
    manutencao.cria_campo("pt_cadpats","codant_3")

    cnx.commit()

def triggers(status):
    print("Triggers")

    numero = 0 if str(status).lower() == "desliga" else 1
    triggers = [
        "UPDATE RDB$TRIGGERS SET RDB$TRIGGER_INACTIVE =  %s WHERE RDB$TRIGGER_NAME = 'TD_PT_MOVBEM_GEN'" % numero,
        "UPDATE RDB$TRIGGERS SET RDB$TRIGGER_INACTIVE =  %s WHERE RDB$TRIGGER_NAME = 'TBD_PT_MOVBEM_BLOQUEIO'" % numero,
        "UPDATE RDB$TRIGGERS SET RDB$TRIGGER_INACTIVE = %s WHERE RDB$TRIGGER_NAME = 'TD_PT_CADPAT_GEN'" % numero,
        "UPDATE RDB$TRIGGERS SET RDB$TRIGGER_INACTIVE = %s WHERE RDB$TRIGGER_NAME = 'TU_PT_CADPAT_BLOQUEIO'" % numero,
        "UPDATE RDB$TRIGGERS SET RDB$TRIGGER_INACTIVE = %s WHERE RDB$TRIGGER_NAME = 'TBI_PT_CADPAT_SINC'" % numero,
        "UPDATE RDB$TRIGGERS SET RDB$TRIGGER_INACTIVE = %s WHERE RDB$TRIGGER_NAME = 'TU_PT_CADPAT_BLOQUEIO'" % numero
    ]

    for cmd in triggers:
        cur_dest.execute(cmd)

    cnx.commit()

def limpa_tabelas():

    triggers("desliga")

    print("Limpando Tabelas")

    tabelas = [
        'delete from pt_tipomov',
        'delete from pt_movbem',
        'delete from pt_cadpat',
        'delete from pt_cadpats',
        'delete from pt_cadpatd',
        'delete from pt_cadpatg',
        'delete from pt_cadsit',
        'delete from pt_cadtip',
        'delete from pt_cadbai',
        "delete from pt_cadajuste",
        "delete from pt_cadresponsavel"
    ]

    for cmd in tabelas:
        cur_dest.execute(cmd)

    cnx.commit()
