from SPARQLWrapper import SPARQLWrapper, JSON
import time
import pandas as pd
import datetime

ENDPOINT = "http://10.33.96.18:7200/repositories/GRAFO_SEFAZMA_PRODUCAO"

#ENDPOINT = "http://10.33.96.18:7200/repositories/GRAFO_REMOTO"

sparql = SPARQLWrapper(ENDPOINT)

def execute(query,sparql=sparql):
    start_time = time.time()
    try:
        print(f"Executando consulta em {datetime.datetime.now()}:\n\n{query}\n\n-------------------------------------------------")
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        sparql.setTimeout(60 * 20)
        results = sparql.query().convert()
    except:
        results = None
    end_time = time.time()
    timeExecution = end_time - start_time
    return timeExecution, results

def size(r):
    if r == None:
        return -1
    return len(r['results']['bindings'])

query= """
select * where { 
	?s ?p ?o .
} limit 100 
"""

t, r = execute(query)

pd_0 = pd.DataFrame([{'tempo':t,'triplas':size(r)}])
pd_0.to_csv("pd0.csv",index=False)
print("pd0.csv salvo com sucesso!!!")
pd_0

ENDPOINT_ONTOLGY = "http://10.33.96.18:7200/repositories/ONTOLOGIA_DOMINIO"
sparql_ontology = SPARQLWrapper(ENDPOINT_ONTOLGY)

query= """
		prefix owl: <http://www.w3.org/2002/07/owl#>
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        select ?class ?label where { 
            {
                ?class a owl:Class.
            }
            UNION{
                ?class a rdfs:Class.
            }
            OPTIONAL{?class rdfs:label ?l}
            BIND(COALESCE(?l,?class) AS ?label)
            FILTER(!CONTAINS(STR(?class),"http://www.w3.org/2000/01/rdf-schema#"))
            FILTER(!CONTAINS(STR(?class),"http://www.w3.org/2001/XMLSchema#"))
            FILTER(!CONTAINS(STR(?class),"http://www.w3.org/1999/02/22-rdf-syntax-ns#"))
            FILTER(!CONTAINS(STR(?class),"http://www.w3.org/2002/07/owl#"))
        } ORDER BY ?label  
"""
print("Consultando classes...")
t, r = execute(query,sparql=sparql_ontology)

classes = []
for result in r['results']['bindings']:
    classes.append({'label': result['label']['value'],'uri':result['class']['value']})
print(len(classes))

results = []
recurso_classe = []
for classe in classes:
    query= f"""
            prefix owl: <http://www.w3.org/2002/07/owl#>
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            select * where {{ 
                ?s a <{classe['uri']}>;
                    ?p ?o.
            }} LIMIT 100
    """
    
    t, r = execute(query)
    if size(r) > 0:
        recurso_classe.append([classe['label'],r['results']['bindings'][0]['s']['value']])
    results.append({'tempo':t,'classe':classe['label'],'triplas':size(r)})
    
pd_1 = pd.DataFrame(results)
pd_1.to_csv("pd1.csv",index=False)
print("pd1.csv salvo com sucesso!!!")

results = []
for recurso in recurso_classe:
    query= f"""
            prefix owl: <http://www.w3.org/2002/07/owl#>
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            select * where {{ 
                <{recurso[1]}> ?p ?o.
            }} LIMIT 100
    """
    
    t, r = execute(query)
    results.append({'tempo':t,'classe':recurso[0],'triplas':size(r)})
    
pd_2 = pd.DataFrame(results)
pd_2.to_csv("pd2.csv",index=False)
print("pd2.csv salvo com sucesso!!!")

results = []
for recurso in recurso_classe:
    query= f"""
            prefix owl: <http://www.w3.org/2002/07/owl#>
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            select * where {{ 
                <{recurso[1]}> rdfs:label ?o.
            }} LIMIT 100
    """
    
    t, r = execute(query)
    results.append({'tempo':t,'classe':recurso[0],'triplas':size(r)})
    
pd_3 = pd.DataFrame(results)
pd_3.to_csv("pd3.csv",index=False)
print("pd3.csv salvo com sucesso!!!")

propriedades = {}
results = []
for classe in classes:
    query= f"""
            prefix owl: <http://www.w3.org/2002/07/owl#>
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            select DISTINCT ?p ?l where {{ 
                {{
                    ?p rdfs:domain <{classe['uri']}>;
                    rdfs:label ?l.
                }}
                UNION{{
                    <{classe['uri']}> rdfs:subClassOf [ rdf:type owl:Restriction ; owl:onProperty ?p; owl:someValuesFrom ?r].
                    ?p rdfs:label ?l.
                }}
                UNION{{
                    <{classe['uri']}> rdfs:subClassOf [ rdf:type owl:Restriction ; owl:onProperty ?p; owl:allValuesFrom ?r].
                    ?p rdfs:label ?l.
                }}
            }}
    """
    t, r = execute(query,sparql=sparql_ontology)
    propriedades[classe['label']] = []
    for result in r['results']['bindings']:
        propriedades[classe['label']].append([result['l']['value'],result['p']['value']])
        p_url = result['p']['value']
        query= f"""
            prefix owl: <http://www.w3.org/2002/07/owl#>
            prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            select * where {{ 
                ?s a <{classe['uri']}>;
                    <{p_url}> ?o.
            }} LIMIT 100
        """
        
        t, r = execute(query)
        results.append({'tempo':t,'classe':classe['label'],'propriedade':result['l']['value'],'triplas':size(r)})
    
pd_4 = pd.DataFrame(results)
pd_4.to_csv("pd4.csv",index=False)
print("pd4.csv salvo com sucesso!!!")

results = []
for recurso in recurso_classe:
    query= f"""
            SELECT * where {{ 
                BIND(<{recurso[1]}> as ?node)
                {{
                    ?node ?p ?o .    
                    filter(?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)
                    filter(isURI(?o))
                }}
                OPTIONAL{{
                    ?s ?p2 ?node.
                    filter(?p2 != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)
                    filter(isURI(?s))
                }}
            }}
    """
    
    t, r = execute(query)
    results.append({'tempo':t,'classe':recurso[0],'recurso':recurso[1],'triplas':size(r)})
    
pd_5 = pd.DataFrame(results)
pd_5.to_csv("pd5.csv",index=False)
print("pd5.csv salvo com sucesso!!!")

results = []
for recurso in recurso_classe:
    query= f"""
            SELECT * where {{ 
                BIND(<{recurso[1]}> as ?node)  
                ?node ?p ?o .    
                filter(?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)
                filter(isURI(?o))
                ?o ?p3 ?o2.
                filter(?p3 != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)
                filter(isURI(?o2))
            }}
    """
    
    t, r = execute(query)
    results.append({'tempo':t,'classe':recurso[0],'recurso':recurso[1],'triplas':size(r)})
pd_6 = pd.DataFrame(results)
pd_6.to_csv("pd6.csv",index=False)
print("pd6.csv salvo com sucesso!!!")
results = []
for recurso in recurso_classe:
    query= f"""

            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            SELECT ?node ?property ?value WHERE{{
                BIND(<{recurso[1]}> as ?node)  
                ?node ?property_ ?value
                FILTER(!isURI(?value))
                BIND((REPLACE(STR(?property_),".*(/|#)","")) as ?property)
            }}
    """
    
    t, r = execute(query)
    results.append({'tempo':t,'classe':recurso[0],'recurso':recurso[1],'triplas':size(r)})
pd_7 = pd.DataFrame(results)
pd_7.to_csv("pd7.csv",index=False)
print("pd7.csv salvo com sucesso!!!")


print(f"Processo concluído com sucesso em {datetime.datetime.now()}!!!")
