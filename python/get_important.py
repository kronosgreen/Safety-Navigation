#EXTRACT IMPORTANT INFO FROM OSM FILE

import xml.etree.ElementTree as ET
from neo4j.v1 import GraphDatabase

print("Reading XML")

osmFile = ET.parse("wyandotte.osm")

print("Done parsing...")

root = osmFile.getroot()

# Look for all roads in osm file
print("Collecting roads...")
roads = []
for way in root.findall('way'):
    # Check to see if way describes a road or something else
    isRoad = False
    for tag in way.findall('tag'):
        if tag.attrib['k'] == 'name':
            isRoad = True
            name = tag.attrib['v']
        if tag.attrib['k'] == 'railway' or tag.attrib['k'] == 'cuisine' or \
        tag.attrib['k'] == 'building' or tag.attrib['k'] == 'landuse' or \
        tag.attrib['k'] == 'leisure' or tag.attrib['k'] == 'aeroway' or \
        tag.attrib['k'] == 'power' or tag.attrib['v'] == 'boundary' or \
        tag.attrib['k'] == 'place':
            isRoad = False
            break
    if not isRoad:
        continue
    # Collect points that make up roads
    points = []
    for point in way.findall('nd'):
        points.append(point.attrib['ref'])
    newRoad = {"name": name, "points": points}
    roads.append(newRoad)

# Look through nodes and grab ones that make up roads
print("Collecting points...")
node_num = 1
nodes = {}
for node in root.findall('node'):
    node_num += 1
    for road in roads:
        for point in road['points']:
            if node.attrib['id'] == point:
                if node.attrib['id'] in nodes:
                    if road["name"] not in nodes[node.attrib['id']]["road"]:
                        nodes[node.attrib['id']]["road"].append(road["name"])
                else:
                    nodes[node.attrib['id']] = {"road": [road["name"]], \
                    "id": point, "lat": node.attrib['lat'], "lon": node.attrib['lon']}
                continue

# Input the points into the graph database and connect them
uri = "bolt://localhost:7687"
user = "neo4j"
password = "wyanOSM"
driver = GraphDatabase.driver(uri, auth=(user, password))

# Adds road point as node in graph
def _add_point(tx, node_key):
    return tx.run("CREATE (a:MapPoint {id: $node_key, latitude: " + \
    nodes[key]['lat'] + ", longitude: " + nodes[key]['lon'] + ", roads: \"" + \
    ' and '.join(nodes[key]['road']) + "\"}) RETURN a", node_key=node_key)

# Creates relationship between nodes
def _connect_road(tx, road):
    for i in range(len(road["points"])-1):
        result = tx.run("MATCH (a:MapPoint), (b:MapPoint) "
                        "WHERE a.id = \"" + str(road["points"][i]) +
                        "\" AND b.id = \"" + str(road["points"][i+1]) + "\" "
                        "CREATE (a)-[r:NEXT]->(b) "
                        "WITH r, point({x: a.longitude, y: a.latitude, crs: 'cartesian'}) AS p1, "
                        "point({x: b.longitude, y: b.latitude, crs: 'cartesian'}) AS p2 "
                        "SET r.distance = distance(p1, p2) "
                        "RETURN type(r);", road=road)
    return "All roads connected"

def test_graph(testid):
    with driver.session() as session:
        return session.run("MATCH (n) "
                        "RETURN n "
                        "LIMIT $testid;", testid=testid)


with driver.session() as session:
    print("Testing")
    test_graph(10)
    print("Adding points...")
    for key in nodes.keys():
        session.write_transaction(_add_point, key)
    print("Connecting roads...")
    for road in roads:
        session.write_transaction(_connect_road, road)


driver.close()
