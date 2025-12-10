#pragma once

#include <iostream>
#include <string>
#include "sqlite3.h"

class DatabaseManager {
private:
    sqlite3* db;
    const std::string dbName = "movie_db.sqlite";


    static int callback(void* data, int argc, char** argv, char** azColName);

    //open and close the database connection
    bool openDatabase();
    void closeDatabase();

public:
    // Constructor and Destructor
    DatabaseManager();
    ~DatabaseManager();

    // Menu Functios
    bool createDatabaseSchema();             // 1) Create Database
    bool importData();                       // 2) Import Data (Will read from your CSV files)
    void performSimpleQuery();               // 3) Simple SELECT
    void performComplexQuery();              // 4) Complex SELECT
    void performUserQuery(const std::string& sql); // 5) User-defined SELECT

    void displayMenu();
};