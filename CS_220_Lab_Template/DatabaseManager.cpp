#include "DatabaseManager.h"
#include <fstream>
#include <sstream>
#include <iomanip>
#include <limits>

int DatabaseManager::callback(void* data, int argc, char** argv, char** azColName) {
    if (*(int*)data == 0) {
        std::cout << "\n------------------------------------------------------------\n";
        for (int i = 0; i < argc; i++) std::cout << std::setw(20) << std::left << azColName[i] << " |";
        std::cout << "\n------------------------------------------------------------\n";
    }
    for (int i = 0; i < argc; i++) std::cout << std::setw(20) << std::left << (argv[i] ? argv[i] : "NULL") << " |";
    std::cout << std::endl;
    (*(int*)data)++;
    return 0;
}

DatabaseManager::DatabaseManager() : db(nullptr) {}
DatabaseManager::~DatabaseManager() { if (db) closeDatabase(); }

bool DatabaseManager::openDatabase() {
    sqlite3_open(dbName.c_str(), &db);
    int rc = sqlite3_exec(db, "PRAGMA foreign_keys = ON;", 0, 0, 0);
    if (rc != SQLITE_OK || !db) {
        std::cerr << "Can't open database: " << (db ? sqlite3_errmsg(db) : "Unknown error") << std::endl;
        return false;
    }
    return true;
}

void DatabaseManager::closeDatabase() { if (db) { sqlite3_close(db); db = nullptr; } }

bool DatabaseManager::createDatabaseSchema() {
    if (!openDatabase()) return false;
    std::cout << "\nAttempting to create database schema in " << dbName << "...\n";
    std::string sql = R"(
        DROP TABLE IF EXISTS MovieStudio;
        DROP TABLE IF EXISTS MovieDirector;
        DROP TABLE IF EXISTS MovieActor;
        DROP TABLE IF EXISTS MovieGenre;
        DROP TABLE IF EXISTS Movie;
        DROP TABLE IF EXISTS Rating;
        DROP TABLE IF EXISTS Country;
        DROP TABLE IF EXISTS Genre;
        DROP TABLE IF EXISTS Person;
        DROP TABLE IF EXISTS Studio;
        CREATE TABLE Rating ( rating_id INTEGER PRIMARY KEY, rating_name CHAR );
        CREATE TABLE Country ( country_id INTEGER PRIMARY KEY, country_name CHAR );
        CREATE TABLE Studio ( Studio_id INTEGER PRIMARY KEY, Studio_name CHAR );
        CREATE TABLE Person ( Person_id INTEGER PRIMARY KEY, Full_name CHAR );
        CREATE TABLE Genre ( genre_id INTEGER PRIMARY KEY, genre_name CHAR );
        CREATE TABLE Movie (
            Movie_id INTEGER PRIMARY KEY, Title CHAR, release_year INTEGER, ownership CHAR, rating_id INTEGER, country_id INTEGER,
            FOREIGN KEY (rating_id) REFERENCES Rating(rating_id), FOREIGN KEY (country_id) REFERENCES Country(country_id)
        );
        CREATE TABLE MovieGenre (
            Movie_id INTEGER, Genre_id INTEGER, PRIMARY KEY (Movie_id, Genre_id),
            FOREIGN KEY (Movie_id) REFERENCES Movie(Movie_id), FOREIGN KEY (Genre_id) REFERENCES Genre(genre_id)
        );
        CREATE TABLE MovieActor (
            Movie_id INTEGER, Person_id INTEGER, Role CHAR, PRIMARY KEY (Movie_id, Person_id),
            FOREIGN KEY (Movie_id) REFERENCES Movie(Movie_id), FOREIGN KEY (Person_id) REFERENCES Person(Person_id)
        );
        CREATE TABLE MovieDirector (
            Movie_id INTEGER, Person_id INTEGER, PRIMARY KEY (Movie_id, Person_id),
            FOREIGN KEY (Movie_id) REFERENCES Movie(Movie_id), FOREIGN KEY (Person_id) REFERENCES Person(Person_id)
        );
        CREATE TABLE MovieStudio (
            Movie_id INTEGER, Studio_id INTEGER, PRIMARY KEY (Movie_id, Studio_id),
            FOREIGN KEY (Movie_id) REFERENCES Movie(Movie_id), FOREIGN KEY (Studio_id) REFERENCES Studio(Studio_id)
        );
    )";
    char* zErrMsg = 0;
    int rc = sqlite3_exec(db, sql.c_str(), 0, 0, &zErrMsg);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error during schema creation: " << zErrMsg << std::endl;
        sqlite3_free(zErrMsg);
        closeDatabase();
        return false;
    }
    std::cout << "Database schema created successfully.\n";
    closeDatabase();
    return true;
}

bool DatabaseManager::importData() {
    if (!openDatabase()) return false;
    std::cout << "\nAttempting to import data from CSV files...\n";

#define EXEC_SQL(sql_string, current_table) \
        do { \
            char *zErrMsg = 0; \
            int rc = sqlite3_exec(db, sql_string.c_str(), 0, 0, &zErrMsg); \
            if (rc != SQLITE_OK) { \
                if (rc != SQLITE_CONSTRAINT) { \
                    std::cerr << "SQL error on INSERT into " << current_table << ": " << zErrMsg << " (Query: " << sql_string << ")" << std::endl; \
                } \
                sqlite3_free(zErrMsg); \
            } else { \
                imported_count++; \
            } \
        } while(0)

    auto clean_quotes = [](std::string& s) {
        if (s.length() >= 2 && s.front() == '"' && s.back() == '"') s = s.substr(1, s.length() - 2);
        };

    auto import_single_value_table = [&](const std::string& filename, const std::string& table, const std::string& id_col, const std::string& name_col) {
        std::ifstream file(filename);
        int imported_count = 0;
        if (file.is_open()) {
            std::string line;
            while (std::getline(file, line)) {
                std::stringstream ss(line);
                std::string id_str, name;
                if (std::getline(ss, id_str, ',') && std::getline(ss, name)) {
                    clean_quotes(name);
                    std::string sql = "INSERT INTO " + table + " (" + id_col + ", " + name_col + ") VALUES (" + id_str + ", '" + name + "');";
                    EXEC_SQL(sql, table);
                }
            }
            std::cout << "Imported " << imported_count << " rows into " << table << " table successfully.\n";
            file.close();
        }
        else { std::cerr << "ERROR: Could not open " << filename << ". Skipping " << table << " import.\n"; }
        };

    import_single_value_table("Rating.csv", "Rating", "rating_id", "rating_name");
    import_single_value_table("Country.csv", "Country", "country_id", "country_name");
    import_single_value_table("Genre.csv", "Genre", "genre_id", "genre_name");
    import_single_value_table("Person.csv", "Person", "Person_id", "Full_name");
    import_single_value_table("Studio.csv", "Studio", "Studio_id", "Studio_name");


    // Movie
    std::ifstream movieFile("Movie.csv");
    if (movieFile.is_open()) {
        std::string line;
        int imported_count = 0;
        const std::string table_name = "Movie";
        while (std::getline(movieFile, line)) {
            std::stringstream ss(line);
            std::string movie_id, title, year, ownership, rating_id, country_id;
            if (std::getline(ss, movie_id, ',') && std::getline(ss, title, ',') && std::getline(ss, year, ',') &&
                std::getline(ss, ownership, ',') && std::getline(ss, rating_id, ',') && std::getline(ss, country_id)) {
                clean_quotes(title);
                clean_quotes(ownership);
                std::string sql = "INSERT INTO Movie (Movie_id, Title, release_year, ownership, rating_id, country_id) VALUES ("
                    + movie_id + ", '" + title + "', " + year + ", '" + ownership + "', " + rating_id + ", " + country_id + ");";
                EXEC_SQL(sql, table_name);
            }
        }
        std::cout << "Imported " << imported_count << " rows into Movie table successfully.\n";
        movieFile.close();
    }
    else { std::cerr << "ERROR: Could not open Movie.csv. Skipping Movie import.\n"; }

    auto import_two_column_bridge = [&](const std::string& filename, const std::string& table, const std::string& col1, const std::string& col2) {
        std::ifstream file(filename);
        int imported_count = 0;
        if (file.is_open()) {
            std::string line;
            while (std::getline(file, line)) {
                std::stringstream ss(line);
                std::string val1, val2;
                if (std::getline(ss, val1, ',') && std::getline(ss, val2)) {
                    std::string sql = "INSERT INTO " + table + " (" + col1 + ", " + col2 + ") VALUES (" + val1 + ", " + val2 + ");";
                    EXEC_SQL(sql, table);
                }
            }
            std::cout << "Imported " << imported_count << " rows into " << table << " table successfully.\n";
            file.close();
        }
        else { std::cerr << "ERROR: Could not open " << filename << ". Skipping " << table << " import.\n"; }
        };

    import_two_column_bridge("MovieGenre.csv", "MovieGenre", "Movie_id", "Genre_id");
    import_two_column_bridge("MovieDirector.csv", "MovieDirector", "Movie_id", "Person_id");
    import_two_column_bridge("MovieStudio.csv", "MovieStudio", "Movie_id", "Studio_id");

    // MovieActor
    std::ifstream movieActorFile("MovieActor.csv");
    if (movieActorFile.is_open()) {
        std::string line;
        int imported_count = 0;
        const std::string table_name = "MovieActor";
        while (std::getline(movieActorFile, line)) {
            std::stringstream ss(line);
            std::string movie_id_str, person_id_str, role;
            if (std::getline(ss, movie_id_str, ',') && std::getline(ss, person_id_str, ',') && std::getline(ss, role)) {
                clean_quotes(role);
                std::string sql = "INSERT INTO MovieActor (Movie_id, Person_id, Role) VALUES ("
                    + movie_id_str + ", " + person_id_str + ", '" + role + "');";
                EXEC_SQL(sql, table_name);
            }
        }
        std::cout << "Imported " << imported_count << " rows into MovieActor table successfully.\n";
        movieActorFile.close();
    }
    else { std::cerr << "ERROR: Could not open MovieActor.csv. Skipping MovieActor import.\n"; }

#undef EXEC_SQL
    closeDatabase();
    return true;
}

void DatabaseManager::performSimpleQuery() {
    if (!openDatabase()) return;
    std::cout << "\n--- 3) Simple SELECT: Displaying all Movie Titles and Release Years ---\n";
    const char* sql = "SELECT Title, release_year FROM Movie ORDER BY release_year DESC LIMIT 10;";
    int row_count = 0;
    char* zErrMsg = 0;
    int rc = sqlite3_exec(db, sql, callback, &row_count, &zErrMsg);
    if (rc != SQLITE_OK) { std::cerr << "SQL error: " << zErrMsg << std::endl; sqlite3_free(zErrMsg); }
    else { std::cout << "\nSuccessfully retrieved " << row_count << " rows.\n"; }
    closeDatabase();
}

void DatabaseManager::performComplexQuery() {
    if (!openDatabase()) return;
    std::cout << "\n--- 4) Complex SELECT: Movie Titles, Ratings, and Country (JOIN) ---\n";
    const char* sql = R"(
        SELECT M.Title, R.rating_name, C.country_name
        FROM Movie AS M
        INNER JOIN Rating AS R ON M.rating_id = R.rating_id
        INNER JOIN Country AS C ON M.country_id = C.country_id
        WHERE R.rating_name = 'PG-13'
        ORDER BY C.country_name, M.Title;
    )";
    int row_count = 0;
    char* zErrMsg = 0;
    int rc = sqlite3_exec(db, sql, callback, &row_count, &zErrMsg);
    if (rc != SQLITE_OK) { std::cerr << "SQL error: " << zErrMsg << std::endl; sqlite3_free(zErrMsg); }
    else { std::cout << "\nSuccessfully retrieved " << row_count << " rows.\n"; }
    closeDatabase();
}

void DatabaseManager::performUserQuery(const std::string& sql) {
    if (!openDatabase()) return;
    if (sql.empty() || sql.find("SELECT") != 0 && sql.find("select") != 0) {
        std::cerr << "Invalid or non-SELECT statement provided.\n";
        closeDatabase();
        return;
    }
    std::cout << "\n--- 5) Executing User-defined Query ---\n";
    std::cout << "SQL: " << sql << std::endl;
    int row_count = 0;
    char* zErrMsg = 0;
    int rc = sqlite3_exec(db, sql.c_str(), callback, &row_count, &zErrMsg);
    if (rc != SQLITE_OK) { std::cerr << "SQL error: " << zErrMsg << std::endl; sqlite3_free(zErrMsg); }
    else { std::cout << "\nSuccessfully retrieved " << row_count << " rows.\n"; }
    closeDatabase();
}