package cognodb_ass_benchmark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

public class Neo4jLoader {
	
    private static final String BASE =
            "src/main/resources/dataset/";

    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
    	String URI = System.getenv("NEO_URI");
    	String USERNAME = System.getenv("NEO_USERNAME");
    	String PASSWORD = System.getenv("NEO_PASSWORD");

        Driver driver = GraphDatabase.driver(
                URI,
                AuthTokens.basic(USERNAME, PASSWORD)
        );

        try (Session session = driver.session()) {

            System.out.println("Connected to Neo4j");
            System.out.println();

            createConstraints(session);

            loadPeople(session);

            loadMovies(session);

            loadRelationships(session);

            System.out.println();
            System.out.println("========== FINAL COUNTS ==========");

            long people = session.run(
                    "MATCH (p:Person) RETURN count(p) AS count"
            ).single().get("count").asLong();

            long movies = session.run(
                    "MATCH (m:Movie) RETURN count(m) AS count"
            ).single().get("count").asLong();

            long relationships = session.run(
                    "MATCH ()-[r]->() RETURN count(r) AS count"
            ).single().get("count").asLong();

            System.out.println(
                    "People: " + people
            );

            System.out.println(
                    "Movies: " + movies
            );

            System.out.println(
                    "Relationships: " + relationships
            );

        } catch (Exception e) {

            e.printStackTrace();

        } finally {

            driver.close();
        }
    }

    private static void createConstraints(
            Session session) {

        session.run(
                "CREATE CONSTRAINT person_id IF NOT EXISTS " +
                "FOR (p:Person) REQUIRE p.id IS UNIQUE"
        ).consume();

        session.run(
                "CREATE CONSTRAINT movie_id IF NOT EXISTS " +
                "FOR (m:Movie) REQUIRE m.id IS UNIQUE"
        ).consume();

        System.out.println(
                "Constraints ready"
        );
    }

    private static void loadPeople(
            Session session) throws Exception {

        String file =
                BASE + "people.csv";

        List<Map<String, Object>> batch =
                new ArrayList<>();

        int count = 0;

        try (BufferedReader br =
                     new BufferedReader(
                             new FileReader(file))) {

            br.readLine();

            String line;

            while ((line = br.readLine()) != null) {

                if (line.trim().isEmpty())
                    continue;

                String[] parts =
                        line.split(",", -1);

                if (parts.length < 2)
                    continue;

                Map<String, Object> row =
                        new HashMap<>();

                row.put(
                        "id",
                        parts[0].trim()
                );

                row.put(
                        "name",
                        parts[1].trim()
                );

                batch.add(row);
                count++;

                if (batch.size() >= BATCH_SIZE) {

                    insertPeople(
                            session,
                            batch
                    );

                    batch.clear();

                    System.out.println(
                            "People loaded: " + count
                    );
                }
            }

            if (!batch.isEmpty()) {

                insertPeople(
                        session,
                        batch
                );
            }
        }

        System.out.println(
                "People completed: " + count
        );
    }

    private static void insertPeople(
            Session session,
            List<Map<String, Object>> batch) {

        session.run(
                "UNWIND $rows AS row " +
                "MERGE (p:Person {id: row.id}) " +
                "SET p.name = row.name",
                Map.of("rows", batch)
        ).consume();
    }

    private static void loadMovies(
            Session session) throws Exception {

        String file =
                BASE + "movies.csv";

        List<Map<String, Object>> batch =
                new ArrayList<>();

        int count = 0;

        try (BufferedReader br =
                     new BufferedReader(
                             new FileReader(file))) {

            br.readLine();

            String line;

            while ((line = br.readLine()) != null) {

                if (line.trim().isEmpty())
                    continue;

                String[] parts =
                        line.split(",", -1);

                if (parts.length < 2)
                    continue;

                Map<String, Object> row =
                        new HashMap<>();

                row.put(
                        "id",
                        parts[0].trim()
                );

                row.put(
                        "title",
                        parts[1].trim()
                );

                batch.add(row);
                count++;

                if (batch.size() >= BATCH_SIZE) {

                    insertMovies(
                            session,
                            batch
                    );

                    batch.clear();

                    System.out.println(
                            "Movies loaded: " + count
                    );
                }
            }

            if (!batch.isEmpty()) {

                insertMovies(
                        session,
                        batch
                );
            }
        }

        System.out.println(
                "Movies completed: " + count
        );
    }

    private static void insertMovies(
            Session session,
            List<Map<String, Object>> batch) {

        session.run(
                "UNWIND $rows AS row " +
                "MERGE (m:Movie {id: row.id}) " +
                "SET m.title = row.title",
                Map.of("rows", batch)
        ).consume();
    }

    private static void loadRelationships(
            Session session) throws Exception {

        String file =
                BASE + "relationships.csv";

        List<Map<String, Object>> batch =
                new ArrayList<>();

        int count = 0;

        try (BufferedReader br =
                     new BufferedReader(
                             new FileReader(file))) {

            br.readLine();

            String line;

            while ((line = br.readLine()) != null) {

                if (line.trim().isEmpty())
                    continue;

                String[] parts =
                        line.split(",", -1);

                if (parts.length < 3)
                    continue;

                Map<String, Object> row =
                        new HashMap<>();

                row.put(
                        "person",
                        parts[0].trim()
                );

                row.put(
                        "movie",
                        parts[1].trim()
                );

                row.put(
                        "type",
                        parts[2].trim()
                );

                batch.add(row);
                count++;

                if (batch.size() >= BATCH_SIZE) {

                    insertRelationships(
                            session,
                            batch
                    );

                    batch.clear();

                    System.out.println(
                            "Relationships loaded: "
                                    + count
                    );
                }
            }

            if (!batch.isEmpty()) {

                insertRelationships(
                        session,
                        batch
                );
            }
        }

        System.out.println(
                "Relationships completed: "
                        + count
        );
    }

    private static void insertRelationships(
            Session session,
            List<Map<String, Object>> batch) {

        session.run(

                "UNWIND $rows AS row " +

                "MATCH (p:Person {id: row.person}) " +

                "MATCH (m:Movie {id: row.movie}) " +

                "FOREACH (x IN CASE " +
                "WHEN row.type = 'ACTED_IN' " +
                "THEN [1] ELSE [] END | " +
                "MERGE (p)-[:ACTED_IN]->(m)) " +

                "FOREACH (x IN CASE " +
                "WHEN row.type = 'DIRECTED' " +
                "THEN [1] ELSE [] END | " +
                "MERGE (p)-[:DIRECTED]->(m))",

                Map.of("rows", batch)

        ).consume();
    }
}