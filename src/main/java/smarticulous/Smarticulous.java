package smarticulous;

import smarticulous.db.Exercise;
import smarticulous.db.Exercise.Question;
import smarticulous.db.Submission;
import smarticulous.db.User;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * The Smarticulous class, implementing a grading system.
 */
public class Smarticulous {

    /**
     * The connection to the underlying DB.
     * <p>
     * null if the db has not yet been opened.
     */
    Connection db;

    /**
     * Open the {@link Smarticulous} SQLite database.
     * <p>
     * This should open the database, creating a new one if necessary, and set the
     * {@link #db} field
     * to the new connection.
     * <p>
     * The open method should make sure the database contains the following tables,
     * creating them if necessary:
     *
     * <table>
     * <caption><em>Table name: <strong>User</strong></em></caption>
     * <tr>
     * <th>Column</th>
     * <th>Type</th>
     * </tr>
     * <tr>
     * <td>UserId</td>
     * <td>Integer (Primary Key)</td>
     * </tr>
     * <tr>
     * <td>Username</td>
     * <td>Text</td>
     * </tr>
     * <tr>
     * <td>Firstname</td>
     * <td>Text</td>
     * </tr>
     * <tr>
     * <td>Lastname</td>
     * <td>Text</td>
     * </tr>
     * <tr>
     * <td>Password</td>
     * <td>Text</td>
     * </tr>
     * </table>
     *
     * <p>
     * <table>
     * <caption><em>Table name: <strong>Exercise</strong></em></caption>
     * <tr>
     * <th>Column</th>
     * <th>Type</th>
     * </tr>
     * <tr>
     * <td>ExerciseId</td>
     * <td>Integer (Primary Key)</td>
     * </tr>
     * <tr>
     * <td>Name</td>
     * <td>Text</td>
     * </tr>
     * <tr>
     * <td>DueDate</td>
     * <td>Integer</td>
     * </tr>
     * </table>
     *
     * <p>
     * <table>
     * <caption><em>Table name: <strong>Question</strong></em></caption>
     * <tr>
     * <th>Column</th>
     * <th>Type</th>
     * </tr>
     * <tr>
     * <td>ExerciseId</td>
     * <td>Integer</td>
     * </tr>
     * <tr>
     * <td>QuestionId</td>
     * <td>Integer</td>
     * </tr>
     * <tr>
     * <td>Name</td>
     * <td>Text</td>
     * </tr>
     * <tr>
     * <td>Desc</td>
     * <td>Text</td>
     * </tr>
     * <tr>
     * <td>Points</td>
     * <td>Integer</td>
     * </tr>
     * </table>
     * In this table the combination of ExerciseId and QuestionId together comprise
     * the primary key.
     *
     * <p>
     * <table>
     * <caption><em>Table name: <strong>Submission</strong></em></caption>
     * <tr>
     * <th>Column</th>
     * <th>Type</th>
     * </tr>
     * <tr>
     * <td>SubmissionId</td>
     * <td>Integer (Primary Key)</td>
     * </tr>
     * <tr>
     * <td>UserId</td>
     * <td>Integer</td>
     * </tr>
     * <tr>
     * <td>ExerciseId</td>
     * <td>Integer</td>
     * </tr>
     * <tr>
     * <td>SubmissionTime</td>
     * <td>Integer</td>
     * </tr>
     * </table>
     *
     * <p>
     * <table>
     * <caption><em>Table name: <strong>QuestionGrade</strong></em></caption>
     * <tr>
     * <th>Column</th>
     * <th>Type</th>
     * </tr>
     * <tr>
     * <td>SubmissionId</td>
     * <td>Integer</td>
     * </tr>
     * <tr>
     * <td>QuestionId</td>
     * <td>Integer</td>
     * </tr>
     * <tr>
     * <td>Grade</td>
     * <td>Real</td>
     * </tr>
     * </table>
     * In this table the combination of SubmissionId and QuestionId together
     * comprise the primary key.
     * 
     * /**
     * Opens the SQLite database with the given JDBC URL. It creates the database if
     * it does not exist and
     * ensures that the required tables are created.
     *
     * @param dburl The JDBC url of the database to open (will be of the form
     *              "jdbc:sqlite:...")
     * @return the new connection
     * @throws SQLException
     */
    public Connection openDB(String dburl) throws SQLException {
        // Get a connection to the database
        db = DriverManager.getConnection(dburl);

        // Define SQL statements for creating tables if they do not exist
        String createUserTableSQL = "CREATE TABLE IF NOT EXISTS User (" +
                "UserId INTEGER PRIMARY KEY," +
                "Username TEXT UNIQUE," +
                "Firstname TEXT," +
                "Lastname TEXT," +
                "Password TEXT" +
                ");";

        String createExerciseTableSQL = "CREATE TABLE IF NOT EXISTS Exercise (" +
                "ExerciseId INTEGER PRIMARY KEY," +
                "Name TEXT," +
                "DueDate INTEGER" +
                ");";

        String createQuestionTableSQL = "CREATE TABLE IF NOT EXISTS Question (" +
                "ExerciseId INTEGER," +
                "QuestionId INTEGER," +
                "Name TEXT," +
                "Desc TEXT," +
                "Points INTEGER," +
                "PRIMARY KEY (ExerciseId, QuestionId)" +
                ");";

        String createSubmissionTableSQL = "CREATE TABLE IF NOT EXISTS Submission (" +
                "SubmissionId INTEGER PRIMARY KEY," +
                "UserId INTEGER," +
                "ExerciseId INTEGER," +
                "SubmissionTime INTEGER" +
                ");";

        String createQuestionGradeTableSQL = "CREATE TABLE IF NOT EXISTS QuestionGrade (" +
                "SubmissionId INTEGER," +
                "QuestionId INTEGER," +
                "Grade REAL," +
                "PRIMARY KEY (SubmissionId, QuestionId)" +
                ");";

        // Execute the SQL statements to create the tables
        try (Statement stmt = db.createStatement()) {
            stmt.executeUpdate(createUserTableSQL);
            stmt.executeUpdate(createExerciseTableSQL);
            stmt.executeUpdate(createQuestionTableSQL);
            stmt.executeUpdate(createSubmissionTableSQL);
            stmt.executeUpdate(createQuestionGradeTableSQL);

        } catch (SQLException e) {
            // Close the connection if an error occurs during table creation
            if (db != null)
                db.close();
            throw e; // throw the exception
        }
        // Return the open connection
        return db;
    }

    /**
     * Close the DB if it is open.
     *
     * @throws SQLException
     */
    public void closeDB() throws SQLException {
        if (db != null) {
            db.close();
            db = null;
        }
    }

    // =========== User Management =============

    /**
     * Add a user to the database / modify an existing user.
     * <p>
     * Add the user to the database if they don't exist. If a user with
     * user.username does exist,
     * update their password and firstname/lastname in the database.
     *
     * @param user
     * @param password
     * @return the userid.
     * @throws SQLException
     */
    public int addOrUpdateUser(User user, String password) throws SQLException {
        // Ensure the db connection is open
        if (db == null) {
            throw new SQLException("DB connection is not established.");
        }
        // Do not allow empty usernames or passwords
        if (user.username == null || user.username.trim().isEmpty() || password == null || password.trim().isEmpty()) {
            return -1;
        }
        // Insert or replace the user using their username, by the user table- Assumes
        // the Username is unique.
        String insertSql = "INSERT INTO User (Username, Firstname, Lastname, Password) " +
                "VALUES (?, ?, ?, ?) " +
                "ON CONFLICT(Username) DO UPDATE SET " +
                "Firstname = excluded.Firstname, " +
                "Lastname = excluded.Lastname, " +
                "Password = excluded.Password;";
        ;
        try (PreparedStatement prpstmt = db.prepareStatement(insertSql)) {
            prpstmt.setString(1, user.username);
            prpstmt.setString(2, user.firstname);
            prpstmt.setString(3, user.lastname);
            prpstmt.setString(4, password);
            prpstmt.executeUpdate(); // Execute the insert or update operation
        }
        // Retrieve and return the UserId of the inserted or updated user
        String idSql = "SELECT UserId FROM User WHERE Username=?";
        try (PreparedStatement stmt = db.prepareStatement(idSql)) {
            stmt.setString(1, user.username);
            try (ResultSet newUserId = stmt.executeQuery()) {
                if (newUserId.next()) {
                    return newUserId.getInt("UserId"); // Return the user's ID
                }
            }
        }
        return -1;
    }

    /**
     * Verify a user's login credentials.
     *
     * @param username
     * @param password
     * @return true if the user exists in the database and the password matches;
     *         false otherwise.
     * @throws SQLException
     *                      <p>
     *                      Note: this is totally insecure. For real-life password
     *                      checking, it's important to store only
     *                      a password hash
     * @see <a href="https://crackstation.net/hashing-security.htm">How to Hash
     *      Passwords Properly</a>
     */
    public boolean verifyLogin(String username, String password) throws SQLException {
        // Check if the db connection is established
        if (db == null) {
            throw new SQLException("DB connection is not established.");
        }

        // SQL query to select the user with the given username and password
        String query = "SELECT COUNT(*) FROM User WHERE Username = ? AND Password = ?";
        try (PreparedStatement stmt = db.prepareStatement(query)) {
            stmt.setString(1, username);
            stmt.setString(2, password);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                // Check if the user exists with the given username and password
                int count = rs.getInt(1);
                return count > 0; // true only if the user exists and the password matches, false otherwise
            }
        }
        return false; // Return false if user not found or any error occurs
    }

    // =========== Exercise Management =============

    /**
     * Add an exercise to the database.
     *
     * @param exercise
     * @return the new exercise id, or -1 if an exercise with this id already
     *         existed in the database.
     * @throws SQLException
     */
    public int addExercise(Exercise exercise) throws SQLException {
        // Initialize the id with -1 to indicate failure by default
        int id = -1;

        // SQL query to check if an exercise already exists with the given ID
        String findSql = "SELECT EXISTS (SELECT 1 FROM Exercise WHERE ExerciseId = ?)";

        try (PreparedStatement prpstmt = db.prepareStatement(findSql)) {
            prpstmt.setInt(1, exercise.id);
            try (ResultSet rs = prpstmt.executeQuery()) {
                if (rs.next() && !rs.getBoolean(1)) { // Check if the exercise does not exist
                    // SQL query to insert a new exercise
                    String insertSql = "INSERT INTO Exercise (ExerciseId, Name, DueDate) VALUES (?, ?, ?)";

                    try (PreparedStatement insertSt = db.prepareStatement(insertSql)) {
                        insertSt.setInt(1, exercise.id);
                        insertSt.setString(2, exercise.name);
                        insertSt.setLong(3, exercise.dueDate.getTime());
                        insertSt.executeUpdate();
                        id = exercise.id; // Set id to the exercise ID indicating success
                    }

                    // Insert associated questions for the exercise
                    for (Question question : exercise.questions) {
                        String insertQuestion = "INSERT INTO Question (ExerciseId, Name, Desc, Points) VALUES (?, ?, ?, ?)";

                        try (PreparedStatement prpQuestion = db.prepareStatement(insertQuestion)) {
                            prpQuestion.setInt(1, exercise.id);
                            prpQuestion.setString(2, question.name);
                            prpQuestion.setString(3, question.desc);
                            prpQuestion.setInt(4, question.points);
                            prpQuestion.executeUpdate();
                        }
                    }
                }
            }
        }
        // Return the exercise ID if added successfully, or -1 if not
        return id;
    }

    /**
     * Return a list of all the exercises in the database.
     * <p>
     * The list should be sorted by exercise id.
     *
     * @return list of all exercises.
     * @throws SQLException
     */
    public List<Exercise> loadExercises() throws SQLException {
        List<Exercise> exercises = new ArrayList<>();

        // SQL command to retrieve all exercises ordered by their ID
        String getExercisesSql = "SELECT * FROM Exercise ORDER BY ExerciseId ASC";
        try (Statement stmt = db.createStatement(); ResultSet rsExercises = stmt.executeQuery(getExercisesSql)) {
            while (rsExercises.next()) {
                // Creating an Exercise object for each row in the result set
                int exerciseId = rsExercises.getInt("ExerciseId");
                Exercise exercise = new Exercise(
                        exerciseId,
                        rsExercises.getString("Name"),
                        new Date(rsExercises.getLong("DueDate")));

                // Attaching questions to the current exercise
                attachQuestionsToExercise(exercise);
                // Adding the Exercise to the list
                exercises.add(exercise);
            }
        }
        // Returns the list of exercises with their questions
        return exercises;
    }

    /**
     * Attaches questions to a given exercise by fetching them from the database.
     * This method queries for all questions associated with the exercise's ID.
     *
     * @param exercise The Exercise to which the questions will be attached.
     * @throws SQLException If any database access errors occur during the query
     *                      execution.
     */
    private void attachQuestionsToExercise(Exercise exercise) throws SQLException {
        // SQL query to retrieve questions related to a specific exercise
        String getQuestionsSql = "SELECT Name, Desc, Points FROM Question WHERE ExerciseId = ?";
        try (PreparedStatement prpstmt = db.prepareStatement(getQuestionsSql)) {
            prpstmt.setInt(1, exercise.id); // Setting the exercise ID as the query parameter
            try (ResultSet rsQuestions = prpstmt.executeQuery()) {
                while (rsQuestions.next()) {
                    // Adding each question to the Exercise
                    exercise.addQuestion(
                            rsQuestions.getString("Name"),
                            rsQuestions.getString("Desc"),
                            rsQuestions.getInt("Points"));
                }
            }
        }
    }

    // ========== Submission Storage ===============

    /**
     * Store a submission in the database.
     * The id field of the submission will be ignored if it is -1.
     * <p>
     * Return -1 if the corresponding user doesn't exist in the database.
     *
     * @param submission
     * @return the submission id.
     * @throws SQLException
     */
    public int storeSubmission(Submission submission) throws SQLException {
        // TODO: Implement
        return -1;
    }

    // ============= Submission Query ===============

    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the latest submission for the given
     * exercise by the given user.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username,
     * Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getLastSubmission(User, Exercise)}
     *
     * @return
     */
    PreparedStatement getLastSubmissionGradesStatement() throws SQLException {
        // TODO: Implement
        return null;
    }

    /**
     * Return a prepared SQL statement that, when executed, will
     * return one row for every question of the <i>best</i> submission for the given
     * exercise by the given user.
     * The best submission is the one whose point total is maximal.
     * <p>
     * The rows should be sorted by QuestionId, and each row should contain:
     * - A column named "SubmissionId" with the submission id.
     * - A column named "QuestionId" with the question id,
     * - A column named "Grade" with the grade for that question.
     * - A column named "SubmissionTime" with the time of submission.
     * <p>
     * Parameter 1 of the prepared statement will be set to the User's username,
     * Parameter 2 to the Exercise Id, and
     * Parameter 3 to the number of questions in the given exercise.
     * <p>
     * This will be used by {@link #getBestSubmission(User, Exercise)}
     *
     */
    PreparedStatement getBestSubmissionGradesStatement() throws SQLException {
        // TODO: Implement
        return null;
    }

    /**
     * Return a submission for the given exercise by the given user that satisfies
     * some condition (as defined by an SQL prepared statement).
     * <p>
     * The prepared statement should accept the user name as parameter 1, the
     * exercise id as parameter 2 and a limit on the
     * number of rows returned as parameter 3, and return a row for each question
     * corresponding to the submission, sorted by questionId.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the
     * database).
     *
     * @param user
     * @param exercise
     * @param stmt
     * @return
     * @throws SQLException
     */
    Submission getSubmission(User user, Exercise exercise, PreparedStatement stmt) throws SQLException {
        stmt.setString(1, user.username);
        stmt.setInt(2, exercise.id);
        stmt.setInt(3, exercise.questions.size());

        ResultSet res = stmt.executeQuery();

        boolean hasNext = res.next();
        if (!hasNext)
            return null;

        int sid = res.getInt("SubmissionId");
        Date submissionTime = new Date(res.getLong("SubmissionTime"));

        float[] grades = new float[exercise.questions.size()];

        for (int i = 0; hasNext; ++i, hasNext = res.next()) {
            grades[i] = res.getFloat("Grade");
        }

        return new Submission(sid, user, exercise, submissionTime, (float[]) grades);
    }

    /**
     * Return the latest submission for the given exercise by the given user.
     * <p>
     * Return null if the user has not submitted the exercise (or is not in the
     * database).
     *
     * @param user
     * @param exercise
     * @return
     * @throws SQLException
     */
    public Submission getLastSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getLastSubmissionGradesStatement());
    }

    /**
     * Return the submission with the highest total grade
     *
     * @param user     the user for which we retrieve the best submission
     * @param exercise the exercise for which we retrieve the best submission
     * @return
     * @throws SQLException
     */
    public Submission getBestSubmission(User user, Exercise exercise) throws SQLException {
        return getSubmission(user, exercise, getBestSubmissionGradesStatement());
    }
}
