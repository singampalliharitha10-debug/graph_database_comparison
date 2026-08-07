package cognodb_ass_benchmark;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CognoDBLoader {

    // ============================================================
    // COGNODB CONNECTION
    // ============================================================

    private static final String URI =
            System.getenv("COGNODB_URI");

    private static final String USERNAME =
            System.getenv("COGNODB_USERNAME");

    private static final String PASSWORD =
            System.getenv("COGNODB_PASSWORD");

    // ============================================================
    // DATASET
    // ============================================================

    private static final String BASE =
            "src/main/resources/dataset/";

    private static final String PEOPLE_FILE =
            BASE + "people.csv";

    private static final String MOVIES_FILE =
            BASE + "movies.csv";

    private static final String RELATIONSHIPS_FILE =
            BASE + "relationships.csv";

    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {

        System.out.println("======================================");
        System.out.println("       COGNODB DATASET LOADER");
        System.out.println("======================================");

        System.out.println("People file: " + PEOPLE_FILE);
        System.out.println("Movies file: " + MOVIES_FILE);
        System.out.println("Relationships file: " + RELATIONSHIPS_FILE);
        System.out.println();

        Driver driver = GraphDatabase.driver(
                URI,
                AuthTokens.basic(USERNAME, PASSWORD)
        );

        try {

            // Test connection
            try (Session session = driver.session()) {

                session.run("RETURN 1").consume();

                System.out.println("Connected to CognoDB");
            }

            System.out.println();
            System.out.println("Creating constraints...");

            createConstraints(driver);

            System.out.println();
            System.out.println("Loading people...");
            loadPeople(driver);

            System.out.println();
            System.out.println("Loading movies...");
            loadMovies(driver);

            System.out.println();
            System.out.println("Loading relationships...");
            loadRelationships(driver);

            System.out.println();
            System.out.println("======================================");
            System.out.println("          FINAL COUNTS");
            System.out.println("======================================");

            printFinalCounts(driver);

            System.out.println();
            System.out.println("======================================");
            System.out.println("       LOADING COMPLETED");
            System.out.println("======================================");

        } catch (Exception e) {

            System.out.println();
            System.out.println("LOADING FAILED");
            e.printStackTrace();

        } finally {

            driver.close();
        }
    }

    // ============================================================
    // CONSTRAINTS
    // ============================================================

    private static void createConstraints(
            Driver driver) {

        try (Session session = driver.session()) {

            try {

                session.run(
                        "CREATE CONSTRAINT person_id_unique IF NOT EXISTS " +
                        "FOR (p:Person) REQUIRE p.person_id IS UNIQUE"
                ).consume();

            } catch (Exception e) {

                System.out.println(
                        "Person constraint: " +
                        e.getMessage()
                );
            }

            try {

                session.run(
                        "CREATE CONSTRAINT movie_id_unique IF NOT EXISTS " +
                        "FOR (m:Movie) REQUIRE m.movie_id IS UNIQUE"
                ).consume();

            } catch (Exception e) {

                System.out.println(
                        "Movie constraint: " +
                        e.getMessage()
                );
            }

            System.out.println("Constraints ready");
        }
    }

    // ============================================================
    // PEOPLE
    // CSV:
    // person_id,name
    // ============================================================

    private static void loadPeople(
            Driver driver) throws Exception {

        List<Map<String, Object>> batch =
                new ArrayList<>();

        long total = 0;

        try (BufferedReader reader =
                     new BufferedReader(
                             new FileReader(PEOPLE_FILE))) {

            String line = reader.readLine();

            // Skip header
            if (line == null) {
                return;
            }

            while ((line = reader.readLine()) != null) {

                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] parts =
                        splitCSV(line);

                if (parts.length < 2) {
                    continue;
                }

                String personId =
                        parts[0].trim();

                String name =
                        parts[1].trim();

                if (personId.isEmpty()) {
                    continue;
                }

                Map<String, Object> row =
                        new HashMap<>();

                row.put("person_id", personId);
                row.put("name", name);

                batch.add(row);

                if (batch.size() >= BATCH_SIZE) {

                    insertPeople(driver, batch);

                    total += batch.size();

                    System.out.println(
                            "People loaded: " + total
                    );

                    batch.clear();
                }
            }
        }

        if (!batch.isEmpty()) {

            insertPeople(driver, batch);

            total += batch.size();

            System.out.println(
                    "People loaded: " + total
            );
        }

        System.out.println(
                "People completed: " + total
        );
    }

    private static void insertPeople(
            Driver driver,
            List<Map<String, Object>> batch) {

        try (Session session = driver.session()) {

            session.run(
                    "UNWIND $rows AS row " +
                    "MERGE (p:Person {person_id: row.person_id}) " +
                    "SET p.name = row.name",
                    Map.of("rows", batch)
            ).consume();
        }
    }

    // ============================================================
    // MOVIES
    // CSV:
    // movie_id,title
    // ============================================================

    private static void loadMovies(
            Driver driver) throws Exception {

        List<Map<String, Object>> batch =
                new ArrayList<>();

        long total = 0;

        try (BufferedReader reader =
                     new BufferedReader(
                             new FileReader(MOVIES_FILE))) {

            String line = reader.readLine();

            // Skip header
            if (line == null) {
                return;
            }

            while ((line = reader.readLine()) != null) {

                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] parts =
                        splitCSV(line);

                if (parts.length < 2) {
                    continue;
                }

                String movieId =
                        parts[0].trim();

                String title =
                        parts[1].trim();

                if (movieId.isEmpty()) {
                    continue;
                }

                Map<String, Object> row =
                        new HashMap<>();

                row.put("movie_id", movieId);
                row.put("title", title);

                batch.add(row);

                if (batch.size() >= BATCH_SIZE) {

                    insertMovies(driver, batch);

                    total += batch.size();

                    System.out.println(
                            "Movies loaded: " + total
                    );

                    batch.clear();
                }
            }
        }

        if (!batch.isEmpty()) {

            insertMovies(driver, batch);

            total += batch.size();

            System.out.println(
                    "Movies loaded: " + total
            );
        }

        System.out.println(
                "Movies completed: " + total
        );
    }

    private static void insertMovies(
            Driver driver,
            List<Map<String, Object>> batch) {

        try (Session session = driver.session()) {

            session.run(
                    "UNWIND $rows AS row " +
                    "MERGE (m:Movie {movie_id: row.movie_id}) " +
                    "SET m.title = row.title",
                    Map.of("rows", batch)
            ).consume();
        }
    }

    // ============================================================
    // RELATIONSHIPS
    //
    // CSV:
    // person_id,movie_id,relationship
    //
    // Example:
    // 123,456,ACTED_IN
    // 123,789,DIRECTED
    // ============================================================

    private static void loadRelationships(
            Driver driver) throws Exception {

        List<Map<String, Object>> actedIn =
                new ArrayList<>();

        List<Map<String, Object>> directed =
                new ArrayList<>();

        long total = 0;

        try (BufferedReader reader =
                     new BufferedReader(
                             new FileReader(
                                     RELATIONSHIPS_FILE))) {

            String line = reader.readLine();

            // Skip header
            if (line == null) {
                return;
            }

            while ((line = reader.readLine()) != null) {

                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] parts =
                        splitCSV(line);

                if (parts.length < 3) {
                    continue;
                }

                String personId =
                        parts[0].trim();

                String movieId =
                        parts[1].trim();

                String relationship =
                        parts[2].trim().toUpperCase();

                if (personId.isEmpty()
                        || movieId.isEmpty()) {
                    continue;
                }

                Map<String, Object> row =
                        new HashMap<>();

                row.put("person_id", personId);
                row.put("movie_id", movieId);

                if (relationship.equals("ACTED_IN")) {

                    actedIn.add(row);

                } else if (relationship.equals("DIRECTED")) {

                    directed.add(row);

                } else {

                    System.out.println(
                            "Skipping unknown relationship: "
                                    + relationship
                    );

                    continue;
                }

                /*
                 * Load ACTED_IN batch
                 */

                if (actedIn.size() >= BATCH_SIZE) {

                    insertActedIn(
                            driver,
                            actedIn
                    );

                    total += actedIn.size();

                    System.out.println(
                            "Relationships processed: "
                                    + total
                    );

                    actedIn.clear();
                }

                /*
                 * Load DIRECTED batch
                 */

                if (directed.size() >= BATCH_SIZE) {

                    insertDirected(
                            driver,
                            directed
                    );

                    total += directed.size();

                    System.out.println(
                            "Relationships processed: "
                                    + total
                    );

                    directed.clear();
                }
            }
        }

        // Remaining ACTED_IN
        if (!actedIn.isEmpty()) {

            insertActedIn(
                    driver,
                    actedIn
            );

            total += actedIn.size();

            System.out.println(
                    "Relationships processed: "
                            + total
            );
        }

        // Remaining DIRECTED
        if (!directed.isEmpty()) {

            insertDirected(
                    driver,
                    directed
            );

            total += directed.size();

            System.out.println(
                    "Relationships processed: "
                            + total
            );
        }

        System.out.println(
                "Relationships processed: "
                        + total
        );
    }

    // ============================================================
    // ACTED_IN
    // ============================================================

    private static void insertActedIn(
            Driver driver,
            List<Map<String, Object>> batch) {

        try (Session session = driver.session()) {

            session.run(
                    "UNWIND $rows AS row " +
                    "MATCH (p:Person {person_id: row.person_id}) " +
                    "MATCH (m:Movie {movie_id: row.movie_id}) " +
                    "MERGE (p)-[:ACTED_IN]->(m)",
                    Map.of("rows", batch)
            ).consume();
        }
    }

    // ============================================================
    // DIRECTED
    // ============================================================

    private static void insertDirected(
            Driver driver,
            List<Map<String, Object>> batch) {

        try (Session session = driver.session()) {

            session.run(
                    "UNWIND $rows AS row " +
                    "MATCH (p:Person {person_id: row.person_id}) " +
                    "MATCH (m:Movie {movie_id: row.movie_id}) " +
                    "MERGE (p)-[:DIRECTED]->(m)",
                    Map.of("rows", batch)
            ).consume();
        }
    }

    // ============================================================
    // FINAL COUNTS
    // ============================================================

    private static void printFinalCounts(
            Driver driver) {

        try (Session session = driver.session()) {

            long people =
                    session.run(
                            "MATCH (p:Person) " +
                            "RETURN count(p) AS count"
                    )
                    .single()
                    .get("count")
                    .asLong();

            long movies =
                    session.run(
                            "MATCH (m:Movie) " +
                            "RETURN count(m) AS count"
                    )
                    .single()
                    .get("count")
                    .asLong();

            long relationships =
                    session.run(
                            "MATCH ()-[r]->() " +
                            "RETURN count(r) AS count"
                    )
                    .single()
                    .get("count")
                    .asLong();

            long actedIn =
                    session.run(
                            "MATCH ()-[r:ACTED_IN]->() " +
                            "RETURN count(r) AS count"
                    )
                    .single()
                    .get("count")
                    .asLong();

            long directed =
                    session.run(
                            "MATCH ()-[r:DIRECTED]->() " +
                            "RETURN count(r) AS count"
                    )
                    .single()
                    .get("count")
                    .asLong();

            System.out.println(
                    "People: " + people
            );

            System.out.println(
                    "Movies: " + movies
            );

            System.out.println(
                    "Relationships: " + relationships
            );

            System.out.println(
                    "ACTED_IN: " + actedIn
            );

            System.out.println(
                    "DIRECTED: " + directed
            );
        }
    }

    // ============================================================
    // SIMPLE CSV SPLITTER
    // ============================================================

    private static String[] splitCSV(
            String line) {

        List<String> values =
                new ArrayList<>();

        StringBuilder current =
                new StringBuilder();

        boolean insideQuotes = false;

        for (int i = 0;
             i < line.length();
             i++) {

            char c = line.charAt(i);

            if (c == '"') {

                insideQuotes =
                        !insideQuotes;

            } else if (c == ','
                    && !insideQuotes) {

                values.add(
                        current.toString()
                );

                current.setLength(0);

            } else {

                current.append(c);
            }
        }

        values.add(
                current.toString()
        );

        return values.toArray(
                new String[0]
        );
    }
}