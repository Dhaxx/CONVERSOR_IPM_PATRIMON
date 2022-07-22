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
        dae_pat = 'v'
        codigo_cpl_pat = row[14]

        cur_dest.execute(insert,(codigo_pat, empresa_pat, codigo_gru_pat, codigo_set_pat, codigo_set_atu_pat, 
                                 chapa_pat, orig_pat, codigo_for_pat, codigo_tip_pat, codigo_sit_pat, discr_pat, 
                                 datae_pat, dtlan_pat, dt_contabil, valaqu_pat, dae_pat, codigo_cpl_pat))
    cnx.commit()
