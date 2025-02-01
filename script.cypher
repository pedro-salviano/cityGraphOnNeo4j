// Importar dataset cidades de São Paulo, dados obtidos no site do IBGE https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2023/UFs/SP/SP_Municipios_2023.zip
// e convertidos em geojson para importação utilizando a ferramenta QGIS


CALL apoc.load.json("file:///SP_Municipios_2023.geojson") YIELD value
WITH value.features AS features
UNWIND features AS feature
CREATE (c:Cidade {
    cod_municipio: feature.properties.CD_MUN,
    municipio: feature.properties.NM_MUN,
    pop: 0,
    coord: point(
        {
            longitude: 0, latitude: 0
        }
    )
})
RETURN c;


// Obter dados de população e coordenadas do OpenStreetMap através do overpassTurbo.
// Script:
// [out:csv( name, ::lat, ::lon, population)];
// area["name"="São Paulo"]["admin_level"="4"]->.estado;
// node["place"~"^(city|town|village)$"](area.estado);
// out body;

// Importar dados do overpass no neo4j para preencher os dados de população e coordenadas do centro urbano
CALL apoc.load.csv('file:///populacao_coords.csv', {header:false, skip:1, sep: '	'}) YIELD list
MATCH (c:Cidade)
WHERE c.municipio = list[0]
SET  c.pop = toInteger(list[3]), c.coord = point({latitude: toFloat(list[1]), longitude: toFloat(list[2])})
RETURN c;


// Ao tentar importar os dados das rodovias foi notada que seria uma tarefa de grande complexidade, e portanto decidi selecionar as 20 cidades mais populosas do dataset
MATCH (c) WHERE c.pop IS NULL DELETE c;
MATCH (c:Cidade) ORDER BY c.pop DESC SKIP 20 DELETE c;

// e pesquisei manualmente os trajetos, levando em consideração em calcular apenas os trajetos entre as cidades imediatamente conectadas entre si do conjunto
// Os resultados foram salvos em um csv
CALL apoc.load.csv("file:///trajetos.csv") YIELD list
MATCH (c1:Cidade{municipio: list[0]}), (c2:Cidade{municipio: list[1]})
MERGE (c1)-[r1:Trajeto{distanciaKM: toFloat(list[2])}]->(c2);


// Definir aleatoriamente o tipo de rodovia, sendo que direções diferentes de uma mesma rodovia podem ter tipos diferentes.
MATCH ()-[r]->()
SET r.tipo = apoc.coll.randomItem(['Federal', 'Estadual', 'Vicinal'])
RETURN r;

//Criar e associar regiões com Cidades
CREATE (RMSP:Regiao{nome: "Região Metropolitana de São Paulo", Decricao: "Na Região Metropolitana de São Paulo se concentram os melhores serviços urbanos e sociais, comércio e serviços sofisticados, instituições de pesquisa e ensino superior de referência, uma complexa rede de atendimento à saúde e a maior oferta de grandes eventos e instituições culturais."})
CREATE (RMC:Regiao{nome: "Região Metropolitana de Campinas", Decricao: "A Região Metropolitana de Campinas comporta um parque industrial moderno, diversificado e composto por segmentos de natureza complementar. Possui uma estrutura agrícola e agroindustrial bastante significativa e desempenha atividades terciárias de expressiva especialização."})
CREATE (VP:Regiao{nome: "Vale do Paraíba", Decricao: "O Vale do Paraíba, localizado entre São Paulo e Rio de Janeiro, é uma região marcada pela indústria automobilística, aeroespacial e tecnológica, com destaque para São José dos Campos. Além da economia, é conhecido por seu patrimônio histórico, religioso e cultural, como Aparecida, centro de peregrinação. A região combina desenvolvimento econômico com áreas de preservação ambiental, como a Serra da Mantiqueira."})
CREATE (BS:Regiao{nome: "Baixada Santista", Decricao: "A Baixada Santista, localizada no estado de São Paulo, é uma região costeira composta por nove municípios, com destaque para Santos, o maior porto da América Latina. Conhecida por sua importância econômica e turística, possui praias, áreas de preservação ambiental e infraestrutura portuária estratégica. A região enfrenta desafios como expansão urbana desordenada e questões ambientais."})
CREATE (ISP:Regiao{nome: "Interior de São Paulo", Decricao: "O interior de São Paulo é uma região diversificada, com cidades desenvolvidas como Campinas, Ribeirão Preto e São José do Rio Preto, que se destacam pela agroindústria, tecnologia e educação. A área combina grandes polos urbanos com áreas rurais produtivas, sendo essencial para a economia do estado. Além disso, oferece qualidade de vida e riqueza cultural, com eventos tradicionais e culinária marcante."})

MATCH (c1:Cidade{municipio: "Ribeirão Preto"}), (ISP:Regiao{nome: "Interior de São Paulo"})
CREATE (c1)-[r:FazParte]->(ISP);
MATCH (c1:Cidade{municipio: "Campinas"}), (ISP:Regiao{nome: "Interior de São Paulo"})
CREATE (c1)-[r:FazParte]->(ISP);
MATCH (c1:Cidade{municipio: "São José dos Campos"}), (ISP:Regiao{nome: "Interior de São Paulo"})
CREATE (c1)-[r:FazParte]->(ISP);
MATCH (c1:Cidade{municipio: "Franca"}), (ISP:Regiao{nome: "Interior de São Paulo"})
CREATE (c1)-[r:FazParte]->(ISP);
MATCH (c1:Cidade{municipio: "Bauru"}), (ISP:Regiao{nome: "Interior de São Paulo"})
CREATE (c1)-[r:FazParte]->(ISP);
MATCH (c1:Cidade{municipio: "São José do Rio Preto"}), (ISP:Regiao{nome: "Interior de São Paulo"})
CREATE (c1)-[r:FazParte]->(ISP);
MATCH (c1:Cidade{municipio: "Sorocaba"}), (ISP:Regiao{nome: "Interior de São Paulo"})
CREATE (c1)-[r:FazParte]->(ISP);
MATCH (c1:Cidade{municipio: "Campinas"}), (ISP:Regiao{nome: "Interior de São Paulo"})
CREATE (c1)-[r:FazParte]->(ISP);
MATCH (c1:Cidade{municipio: "Piracicaba"}), (ISP:Regiao{nome: "Interior de São Paulo"})
CREATE (c1)-[r:FazParte]->(ISP);
MATCH (c1:Cidade{municipio: "Jundiaí"}), (ISP:Regiao{nome: "Interior de São Paulo"})
CREATE (c1)-[r:FazParte]->(ISP);

MATCH (c1:Cidade{municipio: "Santos"}), (BS:Regiao{nome: "Baixada Santista"})
CREATE (c1)-[r:FazParte]->(BS);

MATCH (c1:Cidade{municipio: "São José dos Campos"}), (VP:Regiao{nome: "Vale do Paraíba"})
CREATE (c1)-[r:FazParte]->(VP);

MATCH (c1:Cidade{municipio: "Campinas"}), (RMC:Regiao{nome: "Região Metropolitana de Campinas"})
CREATE (c1)-[r:FazParte]->(RMC);

MATCH (c1:Cidade{municipio: "Carapicuíba"}), (RMSP:Regiao{nome: "Região Metropolitana de São Paulo"})
CREATE (c1)-[r:FazParte]->(RMSP);
MATCH (c1:Cidade{municipio: "Osasco"}), (RMSP:Regiao{nome: "Região Metropolitana de São Paulo"})
CREATE (c1)-[r:FazParte]->(RMSP);
MATCH (c1:Cidade{municipio: "Mogi das Cruzes"}), (RMSP:Regiao{nome: "Região Metropolitana de São Paulo"})
CREATE (c1)-[r:FazParte]->(RMSP);
MATCH (c1:Cidade{municipio: "Itaquaquecetuba"}), (RMSP:Regiao{nome: "Região Metropolitana de São Paulo"})
CREATE (c1)-[r:FazParte]->(RMSP);
MATCH (c1:Cidade{municipio: "Guarulhos"}), (RMSP:Regiao{nome: "Região Metropolitana de São Paulo"})
CREATE (c1)-[r:FazParte]->(RMSP);
MATCH (c1:Cidade{municipio: "Mauá"}), (RMSP:Regiao{nome: "Região Metropolitana de São Paulo"})
CREATE (c1)-[r:FazParte]->(RMSP);
MATCH (c1:Cidade{municipio: "Santo André"}), (RMSP:Regiao{nome: "Região Metropolitana de São Paulo"})
CREATE (c1)-[r:FazParte]->(RMSP);
MATCH (c1:Cidade{municipio: "São Bernardo do Campo"}), (RMSP:Regiao{nome: "Região Metropolitana de São Paulo"})
CREATE (c1)-[r:FazParte]->(RMSP);
MATCH (c1:Cidade{municipio: "Diadema"}), (RMSP:Regiao{nome: "Região Metropolitana de São Paulo"})
CREATE (c1)-[r:FazParte]->(RMSP);
MATCH (c1:Cidade{municipio: "São Paulo"}), (RMSP:Regiao{nome: "Região Metropolitana de São Paulo"})
CREATE (c1)-[r:FazParte]->(RMSP);


// Menor distância entre 2 cidades
MATCH (source:Cidade)-[r:Trajeto]-(target:Cidade)
RETURN gds.graph.project(
  'distanciaEntreCidade',
  source,
  target,
  { relationshipProperties: r { .distanciaKM } }
)

MATCH (c1:Cidade {municipio: 'Ribeirão Preto'}), (c2:Cidade {municipio: 'São Bernardo do Campo'})
CALL gds.shortestPath.dijkstra.stream('distanciaEntreCidade', {
    sourceNode: c1,
    targetNodes: c2,
    relationshipWeightProperty: 'distanciaKM'
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
RETURN
    index,
    gds.util.asNode(sourceNode).municipio AS MunicipioOrigem,
    gds.util.asNode(targetNode).municipio AS MunicipioDestino,
    totalCost AS DistanciaTotal,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).municipio] AS Caminho
ORDER BY index;

// Listar cidades conectadas por rodovia de determinado tipo
MATCH (a)-[r:Trajeto]->(b)
WHERE r.tipo = 'Federal'
RETURN a.municipio, r.tipo, b.municipio;

//Encontrar as cidades que estejam à uma distância máxima definida de uma cidade escolhida
WITH 150 as maxDist
MATCH (c1:Cidade {municipio: 'Sorocaba'}), (c2:Cidade)
CALL gds.shortestPath.dijkstra.stream('distanciaEntreCidade', {
    sourceNode: c1,
    targetNodes: c2,
    relationshipWeightProperty: 'distanciaKM'
})
YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
WHERE totalCost <= maxDist
RETURN
    index,
    gds.util.asNode(sourceNode).municipio AS MunicipioOrigem,
    gds.util.asNode(targetNode).municipio AS MunicipioDestino,
    totalCost AS DistanciaTotal,
    [nodeId IN nodeIds | gds.util.asNode(nodeId).municipio] AS Caminho
ORDER BY index;

//Identificar hubs
MATCH (a:Cidade)-[r:Trajeto]-(b:Cidade)
WITH a, COUNT(DISTINCT r) AS qtdConexoes
WHERE qtdConexoes > 3
RETURN a.municipio, qtdConexoes;

// Criar indíce de texto
CREATE TEXT INDEX idx_municipio FOR (c:Cidade) ON c.municipio ;