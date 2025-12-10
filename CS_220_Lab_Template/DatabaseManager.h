#pragma once

#include <iostream>
#include <string>
#include "sqlite3.h"

class DatabaseManager {
private:
    sqlite3* db;
    const std::string dbName = "movie_db.sqlite"; // The database file name

    // Helper function to handle the sqlite3 callback for SELECT statements
    static int callback(void* data, int argc, char** argv, char** azColName);

    // Internal functions to open and close the database connection
    bool openDatabase();
    void closeDatabase();

public:
    // Constructor and Destructor
    DatabaseManager();
    ~DatabaseManager();

    // Menu Functionality Implementations (Requirement 3: Each task in its own function)
    bool createDatabaseSchema();             // 1) Create Database
    bool importData();                       // 2) Import Data (Will read from your CSV files)
    void performSimpleQuery();               // 3) Simple SELECT
    void performComplexQuery();              // 4) Complex SELECT
    void performUserQuery(const std::string& sql); // 5) User-defined SELECT

    void displayMenu();
};