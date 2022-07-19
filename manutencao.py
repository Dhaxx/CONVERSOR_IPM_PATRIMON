import conexao as cnx

cur_orig = cnx.cur
cur_dest = cnx.cur_d
cur_aux = cnx.cur_a

def cria_campo(nome_tabela, nome_campo):
    resultado = cur_dest.execute(   
        "select count(*) from rdb$relation_fields where rdb$relation_name = '{tabela}' and(rdb$field_name = '{campo}')".format(tabela=nome_tabela.upper(), campo=nome_campo.upper())).fetchone()[0]

    if(resultado == 1): return

    cur_dest.execute("alter table {tabela} add {campo} varchar(20)".format(
            tabela=nome_tabela, campo=nome_campo))

    cnx.commit()

def tabunidade():
    print("Inserindo Unidades...")
    cur_dest.execute("delete from tabunidade")
    insert = cur_dest.prep("""INSERT
                                INTO
                                tabunidade (poder,
                                orgao,
                                unidade,
                                nome,
                                codaudesp,
                                db,
                                fk_taborgao) values (?,?,?,?,?,?,?)""")

    cur_aux.execute("""SELECT * FROM TABUNIDADE t""")

    for row in cur_aux:
        poder = row[0]
        orgao = row[1]
        unidade = row[2]
        nome = row[3]
        codaudesp = row[5]
        db = row[27]
        fk_taborgao = row[28]

        cur_dest.execute(insert, (poder, orgao, unidade, nome, codaudesp, db, fk_taborgao))
    cnx.commit()

def centro_custo():
    print("Inserindo Centros de Custo...")
    cur_dest.execute("delete from centrocusto")
    insert = cur_dest.prep("""INSERT
                                    INTO
                                    CENTROCUSTO (poder,
                                    ORGAO,
                                    DESTINO,
                                    CCUSTO,
                                    DESCR,
                                    CODCCUSTO,
                                    EMPRESA,
                                    RESPONSA,
                                    UNIDADE,
                                    OCULTAR,
                                    PK_CENTROCUSTO,
                                    DB,
                                    FK_TABORGAO,
                                    FK_TABPODER,
                                    FK_TABUNIDADE,
                                    STATUS_OLD,
                                    DESCRICAO_ANTIGA) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")


    cur_aux.execute("""select
                            poder,
                            orgao,
                            destino,
                            ccusto,
                            descr,
                            codccusto,
                            empresa,
                            responsa,
                            unidade,
                            ocultar,
                            pk_centrocusto,
                            db,
                            fk_taborgao,
                            fk_tabpoder,
                            fk_tabunidade,
                            status_old,
                            descricao_antiga
                        from
                            centrocusto c""")

    for row in cur_aux:
        poder = row[0]
        orgao = row[1]
        destino = row[2]
        ccusto = row[3]
        descr = row[4]
        codccusto = row[5]
        empresa = row[6]
        responsa = row[7]
        unidade = row[8]
        ocultar = row[9]
        pk_centrocusto = row[10]
        db = row[11]
        fk_taborgao = row[12]
        fk_tabpoder = row[13]
        fk_tabunidade = row[14]
        status_old = row[15]
        descricao_antiga = row[16]

        cur_dest.execute(insert, (poder, orgao, destino, ccusto, descr, codccusto, empresa, responsa, unidade, ocultar, pk_centrocusto,
                                  db, fk_taborgao, fk_tabpoder, fk_tabunidade, status_old, descricao_antiga[:16]))
    cnx.commit()