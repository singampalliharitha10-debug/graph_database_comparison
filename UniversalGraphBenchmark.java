package cognodb_ass_benchmark;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class UniversalGraphBenchmark {

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

    // ============================================================
    // BENCHMARK SETTINGS
    // ============================================================

    private static final int BATCH_SIZE = 1000;

    private static final int WARMUP_ITERATIONS = 100;

    private static final int MEASURED_ITERATIONS = 100;

    private static final int RANDOM_START_NODES = 100;

    private static final int CONCURRENT_CLIENTS = 10;

    private static final int CONCURRENT_ITERATIONS = 50;


    private static final boolean RESET_DATABASE = true;

    // ============================================================
    // NEO4J ENVIRONMENT
    // ============================================================

    private static final String NEO4J_URI =
            System.getenv("NEO_URI");

    private static final String NEO4J_USERNAME =
            System.getenv("NEO_USERNAME");

    private static final String NEO4J_PASSWORD =
            System.getenv("NEO_PASSWORD");

    // ============================================================
    // COGNODB ENVIRONMENT
    // ============================================================

    private static final String COGNODB_URI =
            System.getenv("COGNODB_URI");

    private static final String COGNODB_USERNAME =
            System.getenv("COGNODB_USERNAME");

    private static final String COGNODB_PASSWORD =
            System.getenv("COGNODB_PASSWORD");


    private static final String MEMGRAPH_HOST =
            System.getenv("MEM_HOST");

    private static final String MEMGRAPH_PORT =
            System.getenv("MEM_PORT");

    private static final String MEMGRAPH_USERNAME =
            System.getenv("MEM_USERNAME");

    private static final String MEMGRAPH_PASSWORD =
            System.getenv("MEM_PASSWORD");

    // ============================================================
    // RESULT OBJECT
    // ============================================================

    static class BenchmarkResult {

        String database;

        long people;

        long movies;

        long relationships;

        long inputRelationshipRows;

        double loadTimeSeconds;

        double nodesPerSecond;

        double relationshipsPerSecond;

        double hop1P50;
        double hop1P95;

        double hop2P50;
        double hop2P95;

        double hop3P50;
        double hop3P95;

        double pointP50;
        double pointP95;

        double indexedP50;
        double indexedP95;

        double filteredP50;
        double filteredP95;

        double aggregationP50;
        double aggregationP95;

        double mixedQPS;

        double clientMemoryMB;
    }

    // ============================================================
    // MAIN
    // ============================================================

    public static void main(String[] args) {

        System.out.println();
        System.out.println(
                "============================================================"
        );

        System.out.println(
                "          UNIVERSAL GRAPH DATABASE BENCHMARK"
        );

        System.out.println(
                "============================================================"
        );

        System.out.println();

        System.out.println(
                "Dataset:"
        );

        System.out.println(
                "People      : " + PEOPLE_FILE
        );

        System.out.println(
                "Movies      : " + MOVIES_FILE
        );

        System.out.println(
                "Relationships: " + RELATIONSHIPS_FILE
        );

        System.out.println();

        List<BenchmarkResult> results =
                new ArrayList<>();

        // ========================================================
        // NEO4J
        // ========================================================

        runNeo4j(results);

        // ========================================================
        // COGNODB
        // ========================================================

        runCognoDB(results);

        // ========================================================
        // MEMGRAPH
        // ========================================================

        runMemgraph(results);

        // ========================================================
        // FINAL COMPARISON
        // ========================================================

        printFinalComparison(results);

        // ========================================================
        // CSV
        // ========================================================

        saveCSV(results);

        System.out.println();
        System.out.println(
                "============================================================"
        );

        System.out.println(
                "                 BENCHMARK FINISHED"
        );

        System.out.println(
                "============================================================"
        );
    }

    // ============================================================
    // NEO4J
    // ============================================================

    private static void runNeo4j(
            List<BenchmarkResult> results) {

        System.out.println();
        System.out.println(
                "============================================================"
        );

        System.out.println(
                "DATABASE: NEO4J"
        );

        System.out.println(
                "============================================================"
        );

        if (!valid(
                NEO4J_URI,
                NEO4J_USERNAME,
                NEO4J_PASSWORD)) {

            System.out.println(
                    "Neo4j environment variables are missing."
            );

            return;
        }

        Driver driver = null;

        try {

            driver =
                    GraphDatabase.driver(
                            NEO4J_URI,
                            AuthTokens.basic(
                                    NEO4J_USERNAME,
                                    NEO4J_PASSWORD
                            )
                    );

            driver.verifyConnectivity();

            System.out.println(
                    "Connected to Neo4j"
            );

            BenchmarkResult result =
                    prepareAndBenchmark(
                            driver,
                            "Neo4j",
                            results
                    );

            results.add(result);

        } catch (Exception e) {

            System.out.println(
                    "Neo4j benchmark failed."
            );

            e.printStackTrace();

        } finally {

            if (driver != null) {
                driver.close();
            }
        }
    }

    // ============================================================
    // COGNODB
    // ============================================================

    private static void runCognoDB(
            List<BenchmarkResult> results) {

        System.out.println();
        System.out.println(
                "============================================================"
        );

        System.out.println(
                "DATABASE: COGNODB"
        );

        System.out.println(
                "============================================================"
        );

        if (!valid(
                COGNODB_URI,
                COGNODB_USERNAME,
                COGNODB_PASSWORD)) {

            System.out.println(
                    "CognoDB environment variables are missing."
            );

            return;
        }

        Driver driver = null;

        try {

            driver =
                    GraphDatabase.driver(
                            COGNODB_URI,
                            AuthTokens.basic(
                                    COGNODB_USERNAME,
                                    COGNODB_PASSWORD
                            )
                    );

            driver.verifyConnectivity();

            System.out.println(
                    "Connected to CognoDB"
            );

            BenchmarkResult result =
                    prepareAndBenchmark(
                            driver,
                            "CognoDB",
                            results
                    );

            results.add(result);

        } catch (Exception e) {

            System.out.println(
                    "CognoDB benchmark failed."
            );

            e.printStackTrace();

        } finally {

            if (driver != null) {
                driver.close();
            }
        }
    }

    // ============================================================
    // MEMGRAPH
    // ============================================================

    private static void runMemgraph(
            List<BenchmarkResult> results) {

        System.out.println();
        System.out.println(
                "============================================================"
        );

        System.out.println(
                "DATABASE: MEMGRAPH"
        );

        System.out.println(
                "============================================================"
        );

        if (!valid(
                MEMGRAPH_HOST,
                MEMGRAPH_PORT,
                MEMGRAPH_USERNAME,
                MEMGRAPH_PASSWORD)) {

            System.out.println(
                    "Memgraph environment variables are missing."
            );

            return;
        }

        Driver driver = null;

        try {

            int port =
                    Integer.parseInt(
                            MEMGRAPH_PORT
                    );

            /*
             * IMPORTANT:
             *
             * Do NOT use:
             *
             * bolt+s://
             *
             * together with manual encryption settings.
             *
             * We use bolt:// here and explicitly enable TLS.
             *
             * trustAllCertificates() is useful for the self-signed
             * certificate situation that caused your PKIX error.
             */

            String uri =
                    "bolt://" +
                    MEMGRAPH_HOST +
                    ":" +
                    port;

            Config config =
                    Config.builder()
                            .withEncryption()
                            .withTrustStrategy(
                                    Config.TrustStrategy
                                            .trustAllCertificates()
                            )
                            .build();

            driver =
                    GraphDatabase.driver(
                            uri,
                            AuthTokens.basic(
                                    MEMGRAPH_USERNAME,
                                    MEMGRAPH_PASSWORD
                            ),
                            config
                    );

            driver.verifyConnectivity();

            System.out.println(
                    "Connected to Memgraph"
            );

            BenchmarkResult result =
                    prepareAndBenchmark(
                            driver,
                            "Memgraph",
                            results
                    );

            results.add(result);

        } catch (Exception e) {

            System.out.println(
                    "Memgraph benchmark failed."
            );

            e.printStackTrace();

        } finally {

            if (driver != null) {
                driver.close();
            }
        }
    }

    // ============================================================
    // PREPARE DATABASE
    // ============================================================

    private static BenchmarkResult prepareAndBenchmark(
            Driver driver,
            String database,
            List<BenchmarkResult> results)
            throws Exception {

        System.out.println();

        // --------------------------------------------------------
        // RESET
        // --------------------------------------------------------

        if (RESET_DATABASE) {

            System.out.println(
                    "Resetting benchmark database..."
            );

            resetDatabase(driver);

            System.out.println(
                    "Database reset completed."
            );
        }

        // --------------------------------------------------------
        // INDEXES
        // --------------------------------------------------------

        createIndexes(
                driver,
                database
        );

        // --------------------------------------------------------
        // LOAD
        // --------------------------------------------------------

        BenchmarkResult result =
                new BenchmarkResult();

        result.database =
                database;

        long loadStart =
                System.nanoTime();

        result.inputRelationshipRows =
                loadDataset(driver);

        long loadEnd =
                System.nanoTime();

        result.loadTimeSeconds =
                (loadEnd - loadStart)
                        / 1_000_000_000.0;

        // --------------------------------------------------------
        // COUNTS
        // --------------------------------------------------------

        getCounts(
                driver,
                result
        );

        if (result.loadTimeSeconds > 0) {

            result.nodesPerSecond =
                    (
                            result.people +
                            result.movies
                    )
                    /
                    result.loadTimeSeconds;

            result.relationshipsPerSecond =
                    result.relationships
                    /
                    result.loadTimeSeconds;
        }

        System.out.println();

        System.out.println(
                "Load time: "
                        + String.format(
                                "%.3f",
                                result.loadTimeSeconds
                        )
                        + " seconds"
        );

        System.out.println(
                "Nodes/sec: "
                        + String.format(
                                "%.2f",
                                result.nodesPerSecond
                        )
        );

        System.out.println(
                "Relationships/sec: "
                        + String.format(
                                "%.2f",
                                result.relationshipsPerSecond
                        )
        );

        // --------------------------------------------------------
        // RANDOM PERSON IDS
        // --------------------------------------------------------

        List<String> randomPersonIds =
                getRandomPersonIds(
                        driver,
                        RANDOM_START_NODES
                );

        if (randomPersonIds.isEmpty()) {

            throw new RuntimeException(
                    "No Person nodes found."
            );
        }

        // --------------------------------------------------------
        // RANDOM MOVIE TITLE
        // --------------------------------------------------------

        String movieTitle =
                getOneMovieTitle(driver);

        String personName =
                getOnePersonName(driver);

        // --------------------------------------------------------
        // READ BENCHMARKS
        // --------------------------------------------------------

        System.out.println();
        System.out.println(
                "REQUIRED READ WORKLOADS"
        );

        System.out.println(
                "------------------------------------------------------------"
        );

        double[] values;

        // ========================================================
        // 1-HOP
        // ========================================================

        values =
                benchmarkRandom(
                        driver,
                        "1-Hop Traversal",
                        """
                        MATCH (p:Person {person_id: $person_id})
                              -[:ACTED_IN]->(m:Movie)
                        RETURN m
                        """,
                        randomPersonIds
                );

        result.hop1P50 =
                values[0];

        result.hop1P95 =
                values[1];

        // ========================================================
        // 2-HOP
        // ========================================================

        values =
                benchmarkRandom(
                        driver,
                        "2-Hop Traversal",
                        """
                        MATCH (p:Person {person_id: $person_id})
                              -[:ACTED_IN]->(m:Movie)
                              -[:ACTED_IN]-(p2:Person)
                        RETURN p2
                        """,
                        randomPersonIds
                );

        result.hop2P50 =
                values[0];

        result.hop2P95 =
                values[1];

        // ========================================================
        // 3-HOP
        // ========================================================

        values =
                benchmarkRandom(
                        driver,
                        "3-Hop Traversal",
                        """
                        MATCH (p:Person {person_id: $person_id})
                              -[:ACTED_IN]->(m:Movie)
                              -[:ACTED_IN]-(p2:Person)
                              -[:ACTED_IN]->(m2:Movie)
                        RETURN m2
                        """,
                        randomPersonIds
                );

        result.hop3P50 =
                values[0];

        result.hop3P95 =
                values[1];

        // ========================================================
        // POINT LOOKUP
        // ========================================================

        values =
                benchmarkSingleParameter(
                        driver,
                        "Point Lookup",
                        """
                        MATCH (p:Person)
                        WHERE p.name = $name
                        RETURN p
                        LIMIT 1
                        """,
                        "name",
                        personName
                );

        result.pointP50 =
                values[0];

        result.pointP95 =
                values[1];

        // ========================================================
        // INDEXED LOOKUP
        // ========================================================

        String indexedPersonId =
                randomPersonIds.get(0);

        values =
                benchmarkSingleParameter(
                        driver,
                        "Indexed Lookup",
                        """
                        MATCH (p:Person {
                            person_id: $person_id
                        })
                        RETURN p
                        """,
                        "person_id",
                        indexedPersonId
                );

        result.indexedP50 =
                values[0];

        result.indexedP95 =
                values[1];

        // ========================================================
        // FILTERED LOOKUP
        // ========================================================

        values =
                benchmarkSingleParameter(
                        driver,
                        "Filtered Movie Lookup",
                        """
                        MATCH (m:Movie)
                        WHERE m.title = $title
                        RETURN m
                        LIMIT 1
                        """,
                        "title",
                        movieTitle
                );

        result.filteredP50 =
                values[0];

        result.filteredP95 =
                values[1];

        // ========================================================
        // AGGREGATION
        // ========================================================

        values =
                benchmarkNoParameter(
                        driver,
                        "Aggregation",
                        """
                        MATCH (p:Person)
                              -[:ACTED_IN]->(m:Movie)
                        RETURN m.movie_id, count(*) AS actors
                        """
                );

        result.aggregationP50 =
                values[0];

        result.aggregationP95 =
                values[1];

        // ========================================================
        // MIXED WORKLOAD
        // ========================================================

        result.mixedQPS =
                runMixedWorkload(
                        driver
                );

        // ========================================================
        // MEMORY
        // ========================================================

        result.clientMemoryMB =
                getMemoryMB();

        return result;
    }

    // ============================================================
    // RESET DATABASE
    // ============================================================

    private static void resetDatabase(
            Driver driver) {

        try (Session session =
                     driver.session()) {

            session.run(
                    "MATCH (n) " +
                    "DETACH DELETE n"
            ).consume();
        }
    }

    // ============================================================
    // CREATE INDEXES
    // ============================================================

    private static void createIndexes(
            Driver driver,
            String database) {

        System.out.println();

        System.out.println(
                "Creating/checking indexes..."
        );

        try (Session session =
                     driver.session()) {

            if (database.equalsIgnoreCase(
                    "Memgraph")) {

                createMemgraphIndexes(
                        session
                );

            } else {

                createNeo4jIndexes(
                        session
                );
            }
        }
    }

    // ============================================================
    // NEO4J / COGNODB INDEXES
    // ============================================================

    private static void createNeo4jIndexes(
            Session session) {

        try {

            session.run(
                    """
                    CREATE INDEX person_id_index IF NOT EXISTS
                    FOR (p:Person) ON (p.person_id)
                    """
            ).consume();

            System.out.println(
                    "Person.person_id index: ready"
            );

        } catch (Exception e) {

            System.out.println(
                    "Person.person_id index: unavailable"
            );
        }

        try {

            session.run(
                    """
                    CREATE INDEX movie_id_index IF NOT EXISTS
                    FOR (m:Movie) ON (m.movie_id)
                    """
            ).consume();

            System.out.println(
                    "Movie.movie_id index: ready"
            );

        } catch (Exception e) {

            System.out.println(
                    "Movie.movie_id index: unavailable"
            );
        }
    }

    // ============================================================
    // MEMGRAPH INDEXES
    // ============================================================

    private static void createMemgraphIndexes(
            Session session) {

        try {

            session.run(
                    "CREATE INDEX ON :Person(person_id)"
            ).consume();

            System.out.println(
                    "Person.person_id index: ready"
            );

        } catch (Exception e) {

            System.out.println(
                    "Person.person_id index already exists."
            );
        }

        try {

            session.run(
                    "CREATE INDEX ON :Movie(movie_id)"
            ).consume();

            System.out.println(
                    "Movie.movie_id index: ready"
            );

        } catch (Exception e) {

            System.out.println(
                    "Movie.movie_id index already exists."
            );
        }
    }

    // ============================================================
    // LOAD COMPLETE DATASET
    // ============================================================

    private static long loadDataset(
            Driver driver)
            throws IOException {

        System.out.println();

        System.out.println(
                "Loading dataset..."
        );

        long relationshipRows =
                0;

        // --------------------------------------------------------
        // PEOPLE
        // --------------------------------------------------------

        long people =
                loadPeople(
                        driver
                );

        System.out.println(
                "People loaded: "
                        + people
        );

        // --------------------------------------------------------
        // MOVIES
        // --------------------------------------------------------

        long movies =
                loadMovies(
                        driver
                );

        System.out.println(
                "Movies loaded: "
                        + movies
        );

        // --------------------------------------------------------
        // RELATIONSHIPS
        // --------------------------------------------------------

        relationshipRows =
                loadRelationships(
                        driver
                );

        System.out.println(
                "Relationship rows loaded: "
                        + relationshipRows
        );

        return relationshipRows;
    }

    // ============================================================
    // LOAD PEOPLE
    // ============================================================

    private static long loadPeople(
            Driver driver)
            throws IOException {

        long count = 0;

        List<Map<String, Object>> batch =
                new ArrayList<>();

        try (
                BufferedReader reader =
                        Files.newBufferedReader(
                                Paths.get(
                                        PEOPLE_FILE
                                )
                        )
        ) {

            reader.readLine();

            String line;

            while (
                    (line =
                            reader.readLine())
                            != null
            ) {

                String[] parts =
                        line.split(
                                ",",
                                2
                        );

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

                if (
                        batch.size()
                                >= BATCH_SIZE
                ) {

                    insertPeople(
                            driver,
                            batch
                    );

                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {

                insertPeople(
                        driver,
                        batch
                );
            }
        }

        return count;
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
                    """
                    UNWIND $people AS p
                    CREATE (person:Person {
                        person_id: p.person_id,
                        name: p.name
                    })
                    """,
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

    private static long loadMovies(
            Driver driver)
            throws IOException {

        long count = 0;

        List<Map<String, Object>> batch =
                new ArrayList<>();

        try (
                BufferedReader reader =
                        Files.newBufferedReader(
                                Paths.get(
                                        MOVIES_FILE
                                )
                        )
        ) {

            reader.readLine();

            String line;

            while (
                    (line =
                            reader.readLine())
                            != null
            ) {

                String[] parts =
                        line.split(
                                ",",
                                2
                        );

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

                if (
                        batch.size()
                                >= BATCH_SIZE
                ) {

                    insertMovies(
                            driver,
                            batch
                    );

                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {

                insertMovies(
                        driver,
                        batch
                );
            }
        }

        return count;
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
                    """
                    UNWIND $movies AS m
                    CREATE (movie:Movie {
                        movie_id: m.movie_id,
                        title: m.title
                    })
                    """,
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

    private static long loadRelationships(
            Driver driver)
            throws IOException {

        long count = 0;

        List<Map<String, Object>> batch =
                new ArrayList<>();

        try (
                BufferedReader reader =
                        Files.newBufferedReader(
                                Paths.get(
                                        RELATIONSHIPS_FILE
                                )
                        )
        ) {

            reader.readLine();

            String line;

            while (
                    (line =
                            reader.readLine())
                            != null
            ) {

                String[] parts =
                        line.split(
                                ",",
                                3
                        );

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

                batch.add(
                        relationship
                );

                count++;

                if (
                        batch.size()
                                >= BATCH_SIZE
                ) {

                    insertRelationships(
                            driver,
                            batch
                    );

                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {

                insertRelationships(
                        driver,
                        batch
                );
            }
        }

        return count;
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
                    """
                    UNWIND $rels AS r
                    MATCH (p:Person {
                        person_id: r.person_id
                    })
                    MATCH (m:Movie {
                        movie_id: r.movie_id
                    })
                    CREATE (p)-[:ACTED_IN]->(m)
                    """,
                    Values.parameters(
                            "rels",
                            relationships
                    )
            ).consume();
        }
    }

    // ============================================================
    // GET COUNTS
    // ============================================================

    private static void getCounts(
            Driver driver,
            BenchmarkResult result) {

        try (Session session =
                     driver.session()) {

            result.people =
                    session.run(
                            """
                            MATCH (p:Person)
                            RETURN count(p) AS count
                            """
                    )
                    .single()
                    .get("count")
                    .asLong();

            result.movies =
                    session.run(
                            """
                            MATCH (m:Movie)
                            RETURN count(m) AS count
                            """
                    )
                    .single()
                    .get("count")
                    .asLong();

            result.relationships =
                    session.run(
                            """
                            MATCH ()-[r:ACTED_IN]->()
                            RETURN count(r) AS count
                            """
                    )
                    .single()
                    .get("count")
                    .asLong();
        }

        System.out.println();

        System.out.println(
                "FINAL DATASET COUNTS"
        );

        System.out.println(
                "People        : "
                        + result.people
        );

        System.out.println(
                "Movies        : "
                        + result.movies
        );

        System.out.println(
                "Relationships : "
                        + result.relationships
        );
    }

    // ============================================================
    // GET RANDOM PERSON IDS
    // ============================================================

    private static List<String> getRandomPersonIds(
            Driver driver,
            int count) {

        List<String> ids =
                new ArrayList<>();

        try (Session session =
                     driver.session()) {

            Result result =
                    session.run(
                            """
                            MATCH (p:Person)
                            RETURN p.person_id AS person_id
                            """
                    );

            while (result.hasNext()) {

                Record record =
                        result.next();

                ids.add(
                        record
                                .get("person_id")
                                .asString()
                );
            }
        }

        Collections.shuffle(
                ids,
                new Random(42)
        );

        if (ids.size() > count) {

            return new ArrayList<>(
                    ids.subList(
                            0,
                            count
                    )
            );
        }

        return ids;
    }

    // ============================================================
    // GET ONE PERSON NAME
    // ============================================================

    private static String getOnePersonName(
            Driver driver) {

        try (Session session =
                     driver.session()) {

            return session.run(
                    """
                    MATCH (p:Person)
                    RETURN p.name AS name
                    LIMIT 1
                    """
            )
            .single()
            .get("name")
            .asString();
        }
    }

    // ============================================================
    // GET ONE MOVIE TITLE
    // ============================================================

    private static String getOneMovieTitle(
            Driver driver) {

        try (Session session =
                     driver.session()) {

            return session.run(
                    """
                    MATCH (m:Movie)
                    RETURN m.title AS title
                    LIMIT 1
                    """
            )
            .single()
            .get("title")
            .asString();
        }
    }

    // ============================================================
    // RANDOM BENCHMARK
    // ============================================================

    private static double[] benchmarkRandom(
            Driver driver,
            String name,
            String query,
            List<String> personIds) {

        System.out.println();
        System.out.println(name);

        List<Long> times =
                new ArrayList<>();

        Random random =
                new Random(42);

        // --------------------------------------------------------
        // WARM-UP
        // --------------------------------------------------------

        for (int i = 0;
             i < WARMUP_ITERATIONS;
             i++) {

            String personId =
                    personIds.get(
                            random.nextInt(
                                    personIds.size()
                            )
                    );

            runQuery(
                    driver,
                    query,
                    Values.parameters(
                            "person_id",
                            personId
                    )
            );
        }

        // --------------------------------------------------------
        // MEASURE
        // --------------------------------------------------------

        for (int i = 0;
             i < MEASURED_ITERATIONS;
             i++) {

            String personId =
                    personIds.get(
                            random.nextInt(
                                    personIds.size()
                            )
                    );

            long start =
                    System.nanoTime();

            runQuery(
                    driver,
                    query,
                    Values.parameters(
                            "person_id",
                            personId
                    )
            );

            long end =
                    System.nanoTime();

            times.add(
                    (end - start)
                            / 1_000_000
            );
        }

        return printPercentiles(
                times
        );
    }

    // ============================================================
    // SINGLE PARAMETER BENCHMARK
    // ============================================================

    private static double[] benchmarkSingleParameter(
            Driver driver,
            String name,
            String query,
            String parameterName,
            String parameterValue) {

        System.out.println();
        System.out.println(name);

        List<Long> times =
                new ArrayList<>();

        // Warm-up

        for (int i = 0;
             i < WARMUP_ITERATIONS;
             i++) {

            runQuery(
                    driver,
                    query,
                    Values.parameters(
                            parameterName,
                            parameterValue
                    )
            );
        }

        // Measure

        for (int i = 0;
             i < MEASURED_ITERATIONS;
             i++) {

            long start =
                    System.nanoTime();

            runQuery(
                    driver,
                    query,
                    Values.parameters(
                            parameterName,
                            parameterValue
                    )
            );

            long end =
                    System.nanoTime();

            times.add(
                    (end - start)
                            / 1_000_000
            );
        }

        return printPercentiles(
                times
        );
    }

    // ============================================================
    // NO PARAMETER BENCHMARK
    // ============================================================

    private static double[] benchmarkNoParameter(
            Driver driver,
            String name,
            String query) {

        System.out.println();
        System.out.println(name);

        List<Long> times =
                new ArrayList<>();

        // Warm-up

        for (int i = 0;
             i < WARMUP_ITERATIONS;
             i++) {

            runQuery(
                    driver,
                    query,
                    Values.parameters()
            );
        }

        // Measure

        for (int i = 0;
             i < MEASURED_ITERATIONS;
             i++) {

            long start =
                    System.nanoTime();

            runQuery(
                    driver,
                    query,
                    Values.parameters()
            );

            long end =
                    System.nanoTime();

            times.add(
                    (end - start)
                            / 1_000_000
            );
        }

        return printPercentiles(
                times
        );
    }

    // ============================================================
    // RUN QUERY
    // ============================================================

    private static void runQuery(
            Driver driver,
            String query,
            Value parameters) {

        try (Session session =
                     driver.session()) {

            session.run(
                    query,
                    parameters
            ).consume();
        }
    }

    // ============================================================
    // PERCENTILES
    // ============================================================

    private static double[] printPercentiles(
            List<Long> times) {

        Collections.sort(times);

        double p50 =
                percentile(
                        times,
                        50
                );

        double p95 =
                percentile(
                        times,
                        95
                );

        System.out.println(
                "Warm-up iterations : "
                        + WARMUP_ITERATIONS
        );

        System.out.println(
                "Measured iterations: "
                        + MEASURED_ITERATIONS
        );

        System.out.println(
                "p50                : "
                        + p50
                        + " ms"
        );

        System.out.println(
                "p95                : "
                        + p95
                        + " ms"
        );

        return new double[]{
                p50,
                p95
        };
    }

    // ============================================================
    // PERCENTILE
    // ============================================================

    private static double percentile(
            List<Long> values,
            double percentile) {

        if (values.isEmpty()) {
            return 0;
        }

        int index =
                (int) Math.ceil(
                        percentile /
                        100.0 *
                        values.size()
                ) - 1;

        if (index < 0) {
            index = 0;
        }

        if (index >= values.size()) {
            index =
                    values.size() - 1;
        }

        return values.get(index);
    }

    // ============================================================
    // MIXED WORKLOAD
    // ============================================================

    private static double runMixedWorkload(
            Driver driver)
            throws InterruptedException {

        System.out.println();
        System.out.println(
                "Mixed Read/Write Workload"
        );

        System.out.println(
                "Clients: "
                        + CONCURRENT_CLIENTS
        );

        System.out.println(
                "Iterations/client: "
                        + CONCURRENT_ITERATIONS
        );

        System.out.println(
                "Read/write mix: 80% / 20%"
        );

        ExecutorService executor =
                Executors.newFixedThreadPool(
                        CONCURRENT_CLIENTS
                );

        AtomicLong successful =
                new AtomicLong();

        AtomicLong failed =
                new AtomicLong();

        List<Future<?>> futures =
                new ArrayList<>();

        long start =
                System.nanoTime();

        for (int client = 0;
             client < CONCURRENT_CLIENTS;
             client++) {

            futures.add(
                    executor.submit(
                            () -> {

                                Random random =
                                        new Random();

                                for (int i = 0;
                                     i < CONCURRENT_ITERATIONS;
                                     i++) {

                                    try (
                                            Session session =
                                                    driver.session()
                                    ) {

                                        // ------------------------------------------------
                                        // 80% READ
                                        // ------------------------------------------------

                                        if (
                                                random.nextInt(100)
                                                        < 80
                                        ) {

                                            session.run(
                                                    """
                                                    MATCH (p:Person)
                                                    RETURN p
                                                    LIMIT 1
                                                    """
                                            ).consume();

                                        }

                                        // ------------------------------------------------
                                        // 20% WRITE
                                        // ------------------------------------------------

                                        else {

                                            session.run(
                                                    """
                                                    CREATE (
                                                        b:BenchmarkTemp {
                                                            id: $id
                                                        }
                                                    )
                                                    """,
                                                    Values.parameters(
                                                            "id",
                                                            UUID
                                                                    .randomUUID()
                                                                    .toString()
                                                    )
                                            ).consume();
                                        }

                                        successful
                                                .incrementAndGet();

                                    } catch (Exception e) {

                                        failed
                                                .incrementAndGet();
                                    }
                                }
                            }
                    )
            );
        }

        for (Future<?> future :
                futures) {

            try {

                future.get();

            } catch (ExecutionException e) {

                failed.incrementAndGet();
            }
        }

        long end =
                System.nanoTime();

        executor.shutdown();

        double seconds =
                (end - start)
                        / 1_000_000_000.0;

        long total =
                successful.get()
                        + failed.get();

        double qps =
                total /
                seconds;

        System.out.println(
                "Total operations: "
                        + total
        );

        System.out.println(
                "Successful: "
                        + successful.get()
        );

        System.out.println(
                "Failed: "
                        + failed.get()
        );

        System.out.println(
                "Total time: "
                        + seconds
                        + " seconds"
        );

        System.out.println(
                "Sustained QPS: "
                        + qps
        );

        // --------------------------------------------------------
        // CLEAN TEMP NODES
        // --------------------------------------------------------

        try (Session session =
                     driver.session()) {

            session.run(
                    """
                    MATCH (b:BenchmarkTemp)
                    DELETE b
                    """
            ).consume();
        }

        return qps;
    }

    // ============================================================
    // MEMORY
    // ============================================================

    private static double getMemoryMB() {

        Runtime runtime =
                Runtime.getRuntime();

        long used =
                runtime.totalMemory()
                        - runtime.freeMemory();

        return used /
                (1024.0 * 1024.0);
    }

    // ============================================================
    // ENVIRONMENT CHECK
    // ============================================================

    private static boolean valid(
            String... values) {

        for (String value :
                values) {

            if (value == null ||
                value.trim().isEmpty()) {

                return false;
            }
        }

        return true;
    }

    // ============================================================
    // FINAL COMPARISON
    // ============================================================

    private static void printFinalComparison(
            List<BenchmarkResult> results) {

        System.out.println();
        System.out.println();
        System.out.println(
                "============================================================"
        );

        System.out.println(
                "                    FINAL COMPARISON"
        );

        System.out.println(
                "============================================================"
        );

        System.out.printf(
                "%-25s %-14s %-14s %-14s%n",
                "Metric",
                "Neo4j",
                "CognoDB",
                "Memgraph"
        );

        System.out.println(
                "------------------------------------------------------------"
        );

        printRow(
                "People",
                results,
                "people"
        );

        printRow(
                "Movies",
                results,
                "movies"
        );

        printRow(
                "Relationships",
                results,
                "relationships"
        );

        printRow(
                "Load time (sec)",
                results,
                "load"
        );

        printRow(
                "Nodes/sec",
                results,
                "nodessec"
        );

        printRow(
                "Relationships/sec",
                results,
                "relssec"
        );

        printRow(
                "1-Hop p50 (ms)",
                results,
                "hop1p50"
        );

        printRow(
                "1-Hop p95 (ms)",
                results,
                "hop1p95"
        );

        printRow(
                "2-Hop p50 (ms)",
                results,
                "hop2p50"
        );

        printRow(
                "2-Hop p95 (ms)",
                results,
                "hop2p95"
        );

        printRow(
                "3-Hop p50 (ms)",
                results,
                "hop3p50"
        );

        printRow(
                "3-Hop p95 (ms)",
                results,
                "hop3p95"
        );

        printRow(
                "Point p50 (ms)",
                results,
                "pointp50"
        );

        printRow(
                "Point p95 (ms)",
                results,
                "pointp95"
        );

        printRow(
                "Indexed p50 (ms)",
                results,
                "indexedp50"
        );

        printRow(
                "Indexed p95 (ms)",
                results,
                "indexedp95"
        );

        printRow(
                "Filtered p50 (ms)",
                results,
                "filteredp50"
        );

        printRow(
                "Filtered p95 (ms)",
                results,
                "filteredp95"
        );

        printRow(
                "Aggregation p50 (ms)",
                results,
                "aggregationp50"
        );

        printRow(
                "Aggregation p95 (ms)",
                results,
                "aggregationp95"
        );

        printRow(
                "Mixed QPS",
                results,
                "qps"
        );

        printRow(
                "Client memory (MB)",
                results,
                "memory"
        );

        System.out.println(
                "------------------------------------------------------------"
        );

        System.out.println(
                "Server storage: not observable through standard Bolt"
        );

        System.out.println(
                "Server memory : not observable through standard Bolt"
        );

        System.out.println(
                "Instance specs: document separately in README"
        );
    }

    // ============================================================
    // PRINT ROW
    // ============================================================

    private static void printRow(
            String metric,
            List<BenchmarkResult> results,
            String type) {

        String neo4j = "-";
        String cognodb = "-";
        String memgraph = "-";

        for (BenchmarkResult result :
                results) {

            String value =
                    getValue(
                            result,
                            type
                    );

            if (
                    result.database.equalsIgnoreCase(
                            "Neo4j"
                    )
            ) {

                neo4j = value;

            } else if (
                    result.database.equalsIgnoreCase(
                            "CognoDB"
                    )
            ) {

                cognodb = value;

            } else if (
                    result.database.equalsIgnoreCase(
                            "Memgraph"
                    )
            ) {

                memgraph = value;
            }
        }

        System.out.printf(
                "%-25s %-14s %-14s %-14s%n",
                metric,
                neo4j,
                cognodb,
                memgraph
        );
    }

    // ============================================================
    // GET VALUE
    // ============================================================

    private static String getValue(
            BenchmarkResult r,
            String type) {

        switch (type) {

            case "people":
                return String.valueOf(
                        r.people
                );

            case "movies":
                return String.valueOf(
                        r.movies
                );

            case "relationships":
                return String.valueOf(
                        r.relationships
                );

            case "load":
                return String.format(
                        "%.3f",
                        r.loadTimeSeconds
                );

            case "nodessec":
                return String.format(
                        "%.2f",
                        r.nodesPerSecond
                );

            case "relssec":
                return String.format(
                        "%.2f",
                        r.relationshipsPerSecond
                );

            case "hop1p50":
                return String.format(
                        "%.2f",
                        r.hop1P50
                );

            case "hop1p95":
                return String.format(
                        "%.2f",
                        r.hop1P95
                );

            case "hop2p50":
                return String.format(
                        "%.2f",
                        r.hop2P50
                );

            case "hop2p95":
                return String.format(
                        "%.2f",
                        r.hop2P95
                );

            case "hop3p50":
                return String.format(
                        "%.2f",
                        r.hop3P50
                );

            case "hop3p95":
                return String.format(
                        "%.2f",
                        r.hop3P95
                );

            case "pointp50":
                return String.format(
                        "%.2f",
                        r.pointP50
                );

            case "pointp95":
                return String.format(
                        "%.2f",
                        r.pointP95
                );

            case "indexedp50":
                return String.format(
                        "%.2f",
                        r.indexedP50
                );

            case "indexedp95":
                return String.format(
                        "%.2f",
                        r.indexedP95
                );

            case "filteredp50":
                return String.format(
                        "%.2f",
                        r.filteredP50
                );

            case "filteredp95":
                return String.format(
                        "%.2f",
                        r.filteredP95
                );

            case "aggregationp50":
                return String.format(
                        "%.2f",
                        r.aggregationP50
                );

            case "aggregationp95":
                return String.format(
                        "%.2f",
                        r.aggregationP95
                );

            case "qps":
                return String.format(
                        "%.2f",
                        r.mixedQPS
                );

            case "memory":
                return String.format(
                        "%.2f",
                        r.clientMemoryMB
                );

            default:
                return "-";
        }
    }

    // ============================================================
    // SAVE CSV
    // ============================================================

    private static void saveCSV(
            List<BenchmarkResult> results) {

        String file =
                "benchmark_results.csv";

        try (
                PrintWriter writer =
                        new PrintWriter(
                                new FileWriter(
                                        file
                                )
                        )
        ) {

            writer.println(
                    "Database," +
                    "People," +
                    "Movies," +
                    "Relationships," +
                    "LoadTimeSeconds," +
                    "NodesPerSecond," +
                    "RelationshipsPerSecond," +
                    "Hop1P50," +
                    "Hop1P95," +
                    "Hop2P50," +
                    "Hop2P95," +
                    "Hop3P50," +
                    "Hop3P95," +
                    "PointP50," +
                    "PointP95," +
                    "IndexedP50," +
                    "IndexedP95," +
                    "FilteredP50," +
                    "FilteredP95," +
                    "AggregationP50," +
                    "AggregationP95," +
                    "MixedQPS," +
                    "ClientMemoryMB"
            );

            for (
                    BenchmarkResult r :
                    results
            ) {

                writer.printf(
                        Locale.US,
                        "%s,%d,%d,%d," +
                        "%.3f,%.2f,%.2f," +
                        "%.2f,%.2f," +
                        "%.2f,%.2f," +
                        "%.2f,%.2f," +
                        "%.2f,%.2f," +
                        "%.2f,%.2f," +
                        "%.2f,%.2f," +
                        "%.2f,%.2f," +
                        "%.2f,%.2f," +
                        "%.2f%n",

                        r.database,

                        r.people,

                        r.movies,

                        r.relationships,

                        r.loadTimeSeconds,

                        r.nodesPerSecond,

                        r.relationshipsPerSecond,

                        r.hop1P50,

                        r.hop1P95,

                        r.hop2P50,

                        r.hop2P95,

                        r.hop3P50,

                        r.hop3P95,

                        r.pointP50,

                        r.pointP95,

                        r.indexedP50,

                        r.indexedP95,

                        r.filteredP50,

                        r.filteredP95,

                        r.aggregationP50,

                        r.aggregationP95,

                        r.mixedQPS,

                        r.clientMemoryMB
                );
            }

            System.out.println();

            System.out.println(
                    "CSV saved: "
                            + file
            );

        } catch (Exception e) {

            System.out.println(
                    "Could not save CSV."
            );

            e.printStackTrace();
        }
    }
}