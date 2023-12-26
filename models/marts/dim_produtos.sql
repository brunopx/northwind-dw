WITH
    stg_categorias AS (
        SELECT
        *
        FROM {{ ref('stg_erp__categorias') }}
    )

    ,stg_fornecedores AS (
        SELECT
        *
        FROM {{ ref('stg_erp__fornecedores') }}
    )

    ,stg_produtos AS (
        SELECT
        *
        FROM {{ ref('stg_erp__produtos') }}
    )

    ,join_tabelas AS (
        SELECT
            stg_produtos.id_produto
            ,stg_produtos.id_fornecedor
            ,stg_produtos.id_categoria
            ,stg_produtos.nome_produto
            ,stg_produtos.quantidade_por_unidade
            ,stg_produtos.preco_por_unidade
            ,stg_produtos.unidades_em_estoque
            ,stg_produtos.unidades_por_ordem
            ,stg_produtos.nivel_reabastecimento
            ,stg_produtos.is_descontinuado
            ,stg_categorias.nome_categoria
            ,stg_categorias.descricao_categoria
            ,stg_fornecedores.nome_fornecedor
            ,stg_fornecedores.contato_fornecedor
            ,stg_fornecedores.contato_funcao
            ,stg_fornecedores.endereco_fornecedor
            ,stg_fornecedores.cidade_fornecedor
            ,stg_fornecedores.regiao_fornecedor
            ,stg_fornecedores.cep_fornecedor
            ,stg_fornecedores.pais_fornecedor
        FROM
        stg_produtos
        LEFT JOIN stg_categorias ON 
            (stg_produtos.id_categoria = stg_categorias.id_categoria)
        LEFT JOIN stg_fornecedores ON
            (stg_produtos.id_fornecedor = stg_fornecedores.id_fornecedor)
    )

    , criar_chaves AS (
        SELECT
        ROW_NUMBER () OVER (ORDER BY id_produto) AS pk_produto
        , *
        FROM join_tabelas
    )

    SELECT * FROM criar_chaves