import conexao as cnx
import base

cur_orig = cnx.cur
cur_dest = cnx.cur_d

def tipos_mov():
    print("Inserindo tipos de movimentação")
    cur_dest.execute("""delete from pt_tipomov""")

    insert = cur_dest.prep("INSERT INTO PT_TIPOMOV (CODIGO_TMV, DESCRICAO_TMV) VALUES (?, ?)")

    valores = [
        ("A", "AQUISIÇÃO"),
        ("B", "BAIXA"),
        ("T", "TRANSFERÊNCIA"),
        ("R", "PR. CONTÁBIL"),
        ("P", "TRANS. PLANO")]

    cur_dest.executemany(insert, valores)
    cnx.commit()

def tipos_ajuste():
    print ("Inserindo Tipos de Ajuste")
    cur_dest.execute("""delete from pt_cadajuste""")
    insert = cur_dest.prep("INSERT INTO PT_CADAJUSTE (CODIGO_AJU, EMPRESA_AJU, DESCRICAO_AJU) VALUES (?, ?, ?)")
    
    valores = [(1, base.empresa, "REAVALIAÇÃO(ANTES DO CORTE)")]

    cur_dest.executemany(insert, valores)
    cnx.commit()

def tipos_baixas():
    print("Inserindo baixas...")
    cur_dest.execute("""delete from pt_cadbai""")

    cur_orig.execute("""SELECT * FROM wpa.tbmotivobaixa""")

    insert = cur_dest.prep("INSERT INTO PT_CADBAI (CODIGO_BAI, EMPRESA_BAI, DESCRICAO_BAI) VALUES (?, ?, ?)")

    for row in cur_orig:
        codigo_bai = row[0]
        empresa_bai = base.empresa
        descricao_bai = row[1].upper()
        cur_dest.execute(insert, (codigo_bai, empresa_bai, descricao_bai))
    cnx.commit()

def tipos_bens():
    print("Inserindo tipos de bens...")
    cur_dest.execute("""delete from pt_cadtip""")

    insert = cur_dest.prep("insert into pt_cadtip(codigo_tip, empresa_tip, descricao_tip) values (?, ?, ?)")

    cur_orig.execute("""select
                            row_number () over (
                            order by clacodigo),
                            cladescricao 
                        from
                            wun.tbclasse t""")

    for row in cur_orig.fetchall():
        codigo_tip = row[0]
        empresa_tip = base.empresa
        descricao_tip = row[1].upper()

        cur_dest.execute(insert, (codigo_tip, empresa_tip, descricao_tip[:60]))
    cnx.commit()

def situacao():
    print("Inserindo situações...")
    cur_dest.execute("""delete from pt_cadsit""")

    insert = cur_dest.prep("insert into pt_cadsit(codigo_sit, empresa_sit, descricao_sit) values (?, ?, ?)")

    cur_orig.execute("""SELECT * FROM wpa.tbestadobem t """)

    for row in cur_orig:
        cur_dest.execute(insert, (row[0], base.empresa, row[1].upper()))
    cnx.commit()

def grupos():
    print ("Convertendo grupos")
    cur_dest.execute("""delete from pt_cadpatg""")
    
    insert = cur_dest.prep("""insert into pt_cadpatg (codigo_gru,empresa_gru,nogru_gru) values (?,?,?)""")

    cur_orig.execute("""SELECT * FROM wpa.tbtipobem t """)

    for row in cur_orig:
        codigo_gru = row[0]
        empresa_gru = base.empresa
        nogru_gru = row[1].upper()[:60]

        cur_dest.execute(insert, (codigo_gru, empresa_gru, nogru_gru))
    cnx.commit()

def responsaveis():
    print("Convertendo responsáveis...")
    cur_dest.execute("""delete from pt_cadresponsavel""")
    insert = cur_dest.prep("""insert into pt_cadresponsavel(codigo_resp, nome_resp, cpf_resp) values(?,?,?)""")

    cur_orig.execute("""SELECT distinct(unicodigo) FROM wpa.tbmovbem t """)

    for row in cur_orig:
        codigo_resp = row[0]
        nome_resp = cur_dest.execute(f"select nome from desfor where codif = {row[0]}").fetchone()[0]
        cpf_resp = cur_dest.execute(f"select INSMF from desfor where codif = {row[0]}").fetchone()[0] 
        cpf_resp = None if len(cpf_resp) > 14 else cpf_resp

        cur_dest.execute(insert, (codigo_resp, nome_resp, cpf_resp))
    cnx.commit()

def unidades():
    print("Inserindo unidades...")
    cur_dest.execute("""DELETE FROM PT_CADPATS""")
    cur_dest.execute("""DELETE FROM PT_CADPATD""")

    insert = cur_dest.prep("""INSERT INTO pt_cadpatd(empresa_des,
	                                                codigo_des,
                                                    nauni_des,
                                                    poder_des,
                                                    orgao_des,
                                                    unidade_des,
                                                    ocultar_des)
                             VALUES (?,?,?,?,?,?,?)""")

    cur_dest.execute("""SELECT * FROM TABUNIDADE t """)

    i = 0
    for row in cur_dest.fetchall():
        i += 1
        poder_des = row[0]
        orgao_des = row[1]
        unidade_des = row[2]
        nauni_des = row[3]
        empresa_des = base.empresa
        codigo_des = i
        ocultar_des = 'N'

        cur_dest.execute(insert, (empresa_des, codigo_des, nauni_des, poder_des, orgao_des, unidade_des, ocultar_des))
    cnx.commit()

def subunidades():
    print("Inserindo subunidades...")
    cur_dest.execute("""delete from pt_cadpat""")
    cur_dest.execute("""delete from pt_cadpats""")

    insert = cur_dest.prep("insert into pt_cadpats (empresa_set, codigo_set, codigo_des_set, noset_set, responsa_set, ocultar_set) values (?, ?, ?, ?, ?, ?)")

    cur_dest.execute("""SELECT
                            a.ccusto,
                            a.empresa,
                            a.unidade,
                            a.descr,
                            a.responsa,
                            a.ocultar,
                            b.CODIGO_DES,
                            a.PODER,
                            a.ORGAO
                        FROM
                            centrocusto a,
                            pt_cadpatd b
                        WHERE
                            a.poder = b.PODER_DES
                            AND a.ORGAO = b.ORGAO_DES
                            AND a.UNIDADE = b.UNIDADE_DES""")

    for row in cur_dest.fetchall():
        empresa_set = row[1]
        codigo_set = row[0]
        codigo_des_set = row[6]
        noset_set = row[3]
        responsa_set = row[4]
        ocultar_set = row[5]

        cur_dest.execute(insert, (empresa_set, codigo_set, codigo_des_set, noset_set, responsa_set, ocultar_set))
    cnx.commit()







    
                        
    
    







