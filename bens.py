import conexao as cnx
import base

cur_orig = cnx.cur
cur_dest = cnx.cur_d

def cadastro():
    print("Convertendo bens Patrimoniais...")

    cur_dest.execute("DELETE FROM PT_MOVBEM")
    cur_dest.execute("DELETE FROM PT_CADPAT")

    insert = cur_dest.prep("""INSERT INTO
                                PT_CADPAT (
                                CODIGO_PAT,
                                EMPRESA_PAT,
                                CODIGO_GRU_PAT,
                                CODIGO_SET_PAT,
                                CODIGO_SET_ATU_PAT,
                                CHAPA_PAT,
                                ORIG_PAT,
                                CODIGO_FOR_PAT,
                                CODIGO_TIP_PAT,
                                CODIGO_SIT_PAT,
                                DISCR_PAT,
                                DATAE_PAT,
                                DTLAN_PAT,
                                DT_CONTABIL,
                                VALAQU_PAT,
                                DAE_PAT,
                                codigo_cpl_pat)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")

    cur_orig.execute("""select
                            a.clicodigo,
                            a.tipcodigo as grupo,
                            a.bemcodigo,
                            lpad(cast(a.bemcodigo as varchar),6,'0') as placa,
                            a.bemcomplemento as descricao,
                            case 
                                when a.sitcodigo = 5 or a.sitcodigo = 1 then 'C'
                                when a.sitcodigo = 2 or a.sitcodigo = 6 then 'D'
                                when a.sitcodigo = 3 or a.sitcodigo = 7 then 'S'
                                when a.sitcodigo = 4 then 'I'
                            end as orig_pat,
                            a.bemdtaincorp,
                            a.bemdtaaquis,
                            a.unicodforn as fornecedor,
                            a.bemdatatomba,
                            a.bemnotafiscal,
                            f.cncdescricao  as setor, --Ver como pegar só o da primeira movimentação
                            b.mobestado as estado,
                            d.mvcvalor as valaqu_pat,
                            substring(cast(d.plncodigodeb as varchar) from 1 for 9) as balco
                        from
                            wpa.tbbem a
                        left join wpa.tbmovbem b on a.bemcodigo = b.bemcodigo 
                        left join wpa.tbmovimentacao c on b.movcodigo  = c.movcodigo 
                        left join wpa.tbmovimentocontabilizacao d on b.movcodigo = d.movcodigo 
                        left join wun.tbcencus f on b.cnccodigo = f.cnccodigo 
                        where c.movtipomovimento = 5""")
                    
    for row in cur_orig:
        codigo_pat = row[2]
        empresa_pat = row[0]
        codigo_gru_pat = row[1]
        codigo_set_pat = cur_dest.execute(f"""select codigo_set from PT_CADPATS where noset_set = '{row[11]}' """).fetchone()[0]
        codigo_set_atu_pat = codigo_set_pat
        chapa_pat = row[3]
        orig_pat = row[5]
        codigo_for_pat = row[8]
        codigo_tip_pat = None
        codigo_sit_pat = row[12]
        discr_pat = row[4][:255]
        datae_pat = row[7]
        dtlan_pat = row[6]
        dt_contabil = row[6]
        valaqu_pat = round(row[13],2) if row[13] != None else None
        dae_pat = 'V'
        codigo_cpl_pat = row[14]

        cur_dest.execute(insert,(codigo_pat, empresa_pat, codigo_gru_pat, codigo_set_pat, codigo_set_atu_pat, 
                                 chapa_pat, orig_pat, codigo_for_pat, codigo_tip_pat, codigo_sit_pat, discr_pat, 
                                 datae_pat, dtlan_pat, dt_contabil, valaqu_pat, dae_pat, codigo_cpl_pat))
    cnx.commit()

def movimentacoes():
    print("Inserindo movimentações de Aquisição...")
    cur_dest.execute("DELETE FROM PT_MOVBEM")

    cur_orig.execute("""select
                            c.bemcodigo as chapa,
                            a.movcodigo as codigo_mov,
                            a.clicodigosolic as empresa,
                            case 
                                when a.movtipomovimento = 1 then 'T'
                                when a.movtipomovimento = 3 then 'A'
                                when a.movtipomovimento = 5 then 'A'
                                when a.movtipomovimento = 6 then 'A'
                                when a.movtipomovimento = 7 then 'B'
                                when a.movtipomovimento = 9 then 'R'
                                when a.movtipomovimento = 10 then 'R'
                                when a.movtipomovimento = 11 then 'A'
                                when a.movtipomovimento = 12 then 'R'
                                when a.movtipomovimento = 20 then 'P'
                                when a.movtipomovimento = 21 then 'T'
                            end as tipo_mov,
                            a.movtipomovimento,
                            a.movdata as data_mov,
                            d.cncdescricao as setor,
                            a.motcodigo  as codigo_bai,
                            c.vlrvalorcontabil as valor_mov,
                            case 
                                when c.vlrtipo = 4 then 'S'
                                when c.vlrtipo <> 4 then Null
                            end as depreciacao_mov
                        from
                            wpa.tbmovimentacao a
                        left join wpa.tbmovbem b on a.movcodigo = b.movcodigo 
                        left join wpa.tbmovbemvlr c on a.movcodigo = c.movcodigo 
                        left join wun.tbcencus d on b.cnccodigo = d.cnccodigo 
                        where
                            movtipomovimento in (21, 1, 3, 20, 5, 6, 7, 9, 10, 11, 12) and c.bemcodigo is not null
                        order by c.bemcodigo, a.movdata""")

    insert = cur_dest.prep("""INSERT INTO
                                pt_movbem (codigo_mov,
                                empresa_mov,
                                codigo_pat_mov,
                                data_mov,
                                tipo_mov,
                                CODIGO_SET_MOV,
                                CODIGO_BAI_MOV,
                                VALOR_MOV,
                                DEPRECIACAO_MOV,
                                DT_CONTABIL)
                            VALUES (?,?,?,?,?,
                                    ?,?,?,?,?)""")
    
    vlr_aquisicao = 0
    chapa_ant = None
    i = 0

    for row in cur_orig:
        i += 1
        codigo_mov = i
        empresa_mov = row[2]
        codigo_pat_mov = row[0]
        data_mov = row[5]
        tipo_mov = row[3]
        codigo_set_mov = cur_dest.execute(f"""select codigo_set from PT_CADPATS where noset_set = '{row[6]}' """).fetchone()[0] if row[6] != None else None
        codigo_bai_mov = row[7]
        
        if row[0] != chapa_ant:
            valor_mov = round(row[8],2)
        else:
            valor_mov = round((vlr_aquisicao - row[8]),2)
            if row[9] == 'S':
                valor_mov = -valor_mov

        depreciacao_mov = row[9]
        dt_contabil = data_mov
        vlr_aquisicao = round(row[8],2)
        chapa_ant = row[0]


        cur_dest.execute(insert,(codigo_mov, empresa_mov, codigo_pat_mov, data_mov, tipo_mov,
                                 codigo_set_mov, codigo_bai_mov, valor_mov, depreciacao_mov, dt_contabil))
    cnx.commit()

def atualiza_cad():
    print("Atualizando cadastro de patrimônios...")
    update = cur_dest.prep("""UPDATE PT_CADPAT SET VALAQU_PAT = ? WHERE CODIGO_PAT = ?""")

    cur_dest.execute("""select DISTINCT CODIGO_PAT_MOV, VALOR_MOV  from pt_movbem where tipo_mov = 'A' """)

    for row in cur_dest.fetchall():
        cur_dest.execute(update,(row[1],row[0]))
    cnx.commit()




        

    