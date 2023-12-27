WITH
    dim_produtos AS (
        SELECT *
        FROM {{ ref('dim_produtos') }}
    )

    ,dim_clientes AS (
        SELECT *
        FROM {{ ref('dim_clientes') }}
    )

    ,pedido_itens AS (
        SELECT *
        FROM {{ ref('int_vendas__pedido_itens') }}
    )

    ,join_tabelas AS (
        SELECT
        pedido_itens.sk_pedido_item
        ,pedido_itens.id_pedido
        ,dim_produtos.pk_produto
        ,pedido_itens.id_funcionario
        ,dim_clientes.pk_cliente
        ,pedido_itens.id_transportadora
        ,pedido_itens.data_do_pedido
        ,pedido_itens.frete
        ,pedido_itens.destinatario
        ,pedido_itens.endereco_destinatario
        ,pedido_itens.cep_destinatario
        ,pedido_itens.cidade_destinatario
        ,pedido_itens.regiao_destinatario
        ,pedido_itens.pais_destinatario
        ,pedido_itens.data_do_envio
        ,pedido_itens.data_requerida_entrega
        ,pedido_itens.desconto_perc
        ,pedido_itens.preco_da_unidade
        ,pedido_itens.quantidade
        ,dim_produtos.nome_produto
        ,dim_produtos.nome_categoria
        ,dim_produtos.nome_fornecedor
        ,dim_produtos.is_descontinuado
        ,dim_clientes.nome_cliente
        FROM pedido_itens
        LEFT JOIN dim_produtos ON
            (pedido_itens.id_produto = dim_produtos.id_produto)
        LEFT JOIN dim_clientes ON
            (pedido_itens.id_cliente = dim_clientes.id_cliente)
    )

    ,transformacoes AS (
        SELECT *
        ,preco_da_unidade * quantidade AS total_bruto
        ,(1 - desconto_perc) * preco_da_unidade * quantidade AS total_liquido
        ,CASE 
            WHEN desconto_perc >0 THEN TRUE
            WHEN desconto_perc = 0 THEN FALSE
            ELSE FALSE 
            END AS is_desconto
        FROM join_tabelas
    )

    select * from transformacoes
    