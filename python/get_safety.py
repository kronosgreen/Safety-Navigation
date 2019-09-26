#EXTRACT SAFETY INFORMATION FROM CSV

from neo4j.v1 import GraphDatabase
import csv

print("Reading Crash Intersections")

# Connect to database
uri = "bolt://localhost:7687"
user = "neo4j"
password = "wyanOSM"
driver = GraphDatabase.driver(uri, auth=(user, password))

# Crash to database function
def _add_safety(tx, crash):
    crash_roads = crash[4].split(" @ ")
    for i in range(1):
        if crash_roads[i].endswith("Rd"):
            crash_roads[i] = crash_roads[i][0:-1] + "oad"
    safe_score = crash[11]
    result = tx.run("MATCH (a:MapPoint)"
                        " WHERE a.roads CONTAINS '" + crash_roads[0] +
                        "' AND a.roads CONTAINS '" + crash_roads[1] +
                        "' SET a.safety = " + str(safe_score) +
                        " RETURN a;")
    print(crash_roads)
    return "Safety Score Added"

# Read crash database
with open('crashes_wyandotte.csv', newline='') as csvfile:
    crashreader = csv.reader(csvfile)
    with driver.session() as session:
        for row in crashreader:
            if row[0] == "POINT_ID":
                continue
            session.write_transaction(_add_safety, row)

    driver.close()
