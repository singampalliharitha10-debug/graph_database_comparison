package cognodb_ass_benchmark;

import org.neo4j.driver.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class MemgraphLoader {

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

    // ============================================================
    // MEMGRAPH CONNECTION
    // ============================================================

    private static final String HOST =
            "63.186.251.121";

    private static final int PORT = 7687;

    // ============================================================
    // MAIN
    // ============================================================

    public static void main(String[] args) {

        System.out.println("==============================================");
        System.out.println("        MEMGRAPH DATASET LOADER");
        System.out.println("==============================================");

        System.out.println("People file: " + PEOPLE_FILE);
        System.out.println("Movies file: " + MOVIES_FILE);
        System.out.println("Relationships file: " + RELATIONSHIPS_FILE);

        Driver driver = null;

        try {

            // ====================================================
            // ENVIRONMENT VARIABLES
            // ====================================================

            String USERNAME =
                    System.getenv("MEM_USERNAME");

            String PASSWORD =
                    System.getenv("MEM_PASSWORD");

            System.out.println();
            System.out.println("HOST = " + HOST);
            System.out.println("PORT = " + PORT);
            System.out.println("USERNAME = " + USERNAME);

            System.out.println(
                    "PASSWORD SET = " +
                    (PASSWORD != null &&
                     !PASSWORD.trim().isEmpty())
            );

            // ====================================================
            // CHECK ENVIRONMENT
            // ====================================================

            if (USERNAME == null ||
                USERNAME.trim().isEmpty()) {

                throw new RuntimeException(
                        "MEM_USERNAME environment variable is missing."
                );
            }

            if (PASSWORD == null ||
                PASSWORD.trim().isEmpty()) {

                throw new RuntimeException(
                        "MEM_PASSWORD environment variable is missing."
                );
            }

            // ====================================================
            // MEMGRAPH SSL CONFIGURATION
            // ====================================================

            Config config =
                    Config.builder()
                            .withEncryption()
                            .withTrustStrategy(
                                    Config.TrustStrategy
                                            .trustAllCertificates()
                            )
                            .build();

            // ====================================================
            // CONNECT TO MEMGRAPH
            // ====================================================

            driver = GraphDatabase.driver(
                    "bolt://" + HOST + ":" + PORT,
                    AuthTokens.basic(
                            USERNAME,
                            PASSWORD
                    ),
                    config
            );

            driver.verifyConnectivity();

            System.out.println();
            System.out.println(
                    "Connected to Memgraph successfully!"
            );

            // ====================================================
            // CREATE INDEXES
            // ====================================================

            createIndexes(driver);

            // ====================================================
            // LOAD PEOPLE
            // ====================================================

            loadPeople(driver);

            // ====================================================
            // LOAD MOVIES
            // ====================================================

            loadMovies(driver);

            // ====================================================
            // LOAD RELATIONSHIPS
            // ====================================================

            loadRelationships(driver);

            // ====================================================
            // FINAL COUNTS
            // ====================================================

            printFinalCounts(driver);

            System.out.println();
            System.out.println("==============================================");
            System.out.println("MEMGRAPH LOADING COMPLETED");
            System.out.println("==============================================");

        } catch (Exception e) {

            System.out.println();
            System.out.println("==============================================");
            System.out.println("ERROR CONNECTING/LOADING MEMGRAPH");
            System.out.println("==============================================");

            e.printStackTrace();

        } finally {

            if (driver != null) {
                driver.close();
            }
        }
    }

    // ============================================================
    // CREATE INDEXES
    // ============================================================

    private static void createIndexes(
            Driver driver) {

        System.out.println();
        System.out.println("Creating indexes...");

        try (Session session =
                     driver.session()) {

            try {

                session.run(
                        "CREATE INDEX ON :Person(person_id)"
                ).consume();

                System.out.println(
                        "Person index created."
                );

            } catch (Exception e) {

                System.out.println(
                        "Person index already exists " +
                        "or could not be created."
                );
            }

            try {

                session.run(
                        "CREATE INDEX ON :Movie(movie_id)"
                ).consume();

                System.out.println(
                        "Movie index created."
                );

            } catch (Exception e) {

                System.out.println(
                        "Movie index already exists " +
                        "or could not be created."
                );
            }
        }
    }

    // ============================================================
    // LOAD PEOPLE
    // ============================================================

    private static void loadPeople(
            Driver driver)
            throws IOException {

        System.out.println();
        System.out.println("==============================================");
        System.out.println("Loading people...");
        System.out.println("==============================================");

        List<Map<String, Object>> batch =
                new ArrayList<>();

        long count = 0;

        try (BufferedReader reader =
                     Files.newBufferedReader(
                             Paths.get(PEOPLE_FILE)
                     )) {

            String line;

            reader.readLine();

            while ((line =
                    reader.readLine()) != null) {

                String[] parts =
                        line.split(",", 2);

                if (parts.length < 2) {
                    continue;
                }

                Map<String, Object> person =
                        new HashMap<>();

                person.put(
                        "person_id",
                        parts[0].trim()
                );

                person.put(
                        "name",
                        parts[1].trim()
                );

                batch.add(person);

                count++;

                if (batch.size() >= BATCH_SIZE) {

                    insertPeople(
                            driver,
                            batch
                    );

                    System.out.println(
                            "People loaded: " + count
                    );

                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {

                insertPeople(
                        driver,
                        batch
                );

                System.out.println(
                        "People loaded: " + count
                );
            }
        }

        System.out.println(
                "People completed: " + count
        );
    }

    // ============================================================
    // INSERT PEOPLE
    // ============================================================

    private static void insertPeople(
            Driver driver,
            List<Map<String, Object>> people) {

        try (Session session =
                     driver.session()) {

            session.run(
                    "UNWIND $people AS p " +
                    "MERGE (person:Person " +
                    "{person_id: p.person_id}) " +
                    "SET person.name = p.name",
                    Values.parameters(
                            "people",
                            people
                    )
            ).consume();
        }
    }

    // ============================================================
    // LOAD MOVIES
    // ============================================================

    private static void loadMovies(
            Driver driver)
            throws IOException {

        System.out.println();
        System.out.println("==============================================");
        System.out.println("Loading movies...");
        System.out.println("==============================================");

        List<Map<String, Object>> batch =
                new ArrayList<>();

        long count = 0;

        try (BufferedReader reader =
                     Files.newBufferedReader(
                             Paths.get(MOVIES_FILE)
                     )) {

            String line;

            reader.readLine();

            while ((line =
                    reader.readLine()) != null) {

                String[] parts =
                        line.split(",", 2);

                if (parts.length < 2) {
                    continue;
                }

                Map<String, Object> movie =
                        new HashMap<>();

                movie.put(
                        "movie_id",
                        parts[0].trim()
                );

                movie.put(
                        "title",
                        parts[1].trim()
                );

                batch.add(movie);

                count++;

                if (batch.size() >= BATCH_SIZE) {

                    insertMovies(
                            driver,
                            batch
                    );

                    System.out.println(
                            "Movies loaded: " + count
                    );

                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {

                insertMovies(
                        driver,
                        batch
                );

                System.out.println(
                        "Movies loaded: " + count
                );
            }
        }

        System.out.println(
                "Movies completed: " + count
        );
    }

    // ============================================================
    // INSERT MOVIES
    // ============================================================

    private static void insertMovies(
            Driver driver,
            List<Map<String, Object>> movies) {

        try (Session session =
                     driver.session()) {

            session.run(
                    "UNWIND $movies AS m " +
                    "MERGE (movie:Movie " +
                    "{movie_id: m.movie_id}) " +
                    "SET movie.title = m.title",
                    Values.parameters(
                            "movies",
                            movies
                    )
            ).consume();
        }
    }

    // ============================================================
    // LOAD RELATIONSHIPS
    // ============================================================

    private static void loadRelationships(
            Driver driver)
            throws IOException {

        System.out.println();
        System.out.println("==============================================");
        System.out.println("Loading relationships...");
        System.out.println("==============================================");

        List<Map<String, Object>> batch =
                new ArrayList<>();

        long count = 0;

        try (BufferedReader reader =
                     Files.newBufferedReader(
                             Paths.get(
                                     RELATIONSHIPS_FILE
                             )
                     )) {

            String line;

            reader.readLine();

            while ((line =
                    reader.readLine()) != null) {

                String[] parts =
                        line.split(",", 3);

                if (parts.length < 2) {
                    continue;
                }

                Map<String, Object> relationship =
                        new HashMap<>();

                relationship.put(
                        "person_id",
                        parts[0].trim()
                );

                relationship.put(
                        "movie_id",
                        parts[1].trim()
                );

                batch.add(relationship);

                count++;

                if (batch.size() >= BATCH_SIZE) {

                    insertRelationships(
                            driver,
                            batch
                    );

                    System.out.println(
                            "Relationships loaded: " +
                            count
                    );

                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {

                insertRelationships(
                        driver,
                        batch
                );

                System.out.println(
                        "Relationships loaded: " +
                        count
                );
            }
        }

        System.out.println(
                "Relationships completed: " + count
        );
    }

    // ============================================================
    // INSERT RELATIONSHIPS
    // ============================================================

    private static void insertRelationships(
            Driver driver,
            List<Map<String, Object>> relationships) {

        try (Session session =
                     driver.session()) {

            session.run(
                    "UNWIND $rels AS r " +
                    "MATCH (p:Person " +
                    "{person_id: r.person_id}) " +
                    "MATCH (m:Movie " +
                    "{movie_id: r.movie_id}) " +
                    "MERGE (p)-[:ACTED_IN]->(m)",
                    Values.parameters(
                            "rels",
                            relationships
                    )
            ).consume();
        }
    }

    // ============================================================
    // FINAL COUNTS
    // ============================================================

    private static void printFinalCounts(
            Driver driver) {

        System.out.println();
        System.out.println("==============================================");
        System.out.println("FINAL COUNTS");
        System.out.println("==============================================");

        try (Session session =
                     driver.session()) {

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
                            "MATCH ()-[r:ACTED_IN]->() " +
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
                    "Relationships: " +
                    relationships
            );
        }
    }
}