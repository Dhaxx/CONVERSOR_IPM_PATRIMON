import base
import cadastros
import manutencao

def main():
    # base.limpa_tabelas()
    # manutencao.cria_campo("centrocusto", "DESCRICAO_ANTIGA")
    # manutencao.cria_campo("centrocusto", "STATUS_OLD")
    # manutencao.tabunidade()
    # manutencao.centro_custo()

    ## INSERINDO DADOS ##
    # cadastros.tipos_mov()
    # cadastros.tipos_ajuste()
    # cadastros.tipos_baixas()
    # cadastros.tipos_bens()
    # cadastros.situacao()
    # cadastros.tipos_bens()
    # cadastros.grupos()
    # cadastros.responsaveis()
    # cadastros.unidades()
    # cadastros.subunidades()

    base.triggers("liga")


if __name__ == "__main__":
    main()