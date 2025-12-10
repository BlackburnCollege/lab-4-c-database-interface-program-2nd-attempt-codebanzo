#include <iostream>
#include "sqlite3.h"

#include "DatabaseManager.h"
#include <limits> // Required for cin.ignore

// --- Main Menu Display ---
void DatabaseManager::displayMenu() {
    std::cout << "\n============================================\n";
    std::cout << "          Movie Database Menu \n";
    std::cout << "============================================\n";
    std::cout << "1) Create Database Schema.\n";
    std::cout << "2) Import Data (Requires CSV files).\n";
    std::cout << "3) Perform Simple SELECT (Movie Titles).\n";
    std::cout << "4) Perform Complex SELECT (Movie Title, Rating, Country).\n";
    std::cout << "5) Perform User-defined SELECT statement.\n";
    std::cout << "6) Exit.\n";
    std::cout << "--------------------------------------------\n";
    std::cout << "Enter your choice: ";
}

int main() {
    // Fulfills Requirement 4: Create at least 1 object of the self-defined class type.
    DatabaseManager dbManager;

    int choice = 0;
    std::string userSql;

    do {
        dbManager.displayMenu();

        if (!(std::cin >> choice)) {
            // Handle bad input (like characters)
            std::cin.clear();
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            choice = 0;
            std::cout << "\nInvalid input. Please enter a number.\n";
            continue;
        }

        // Consume the rest of the line (newline character) after reading the integer
        std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

        switch (choice) {
        case 1:
            dbManager.createDatabaseSchema();
            break;
        case 2:
            dbManager.importData();
            break;
        case 3:
            dbManager.performSimpleQuery();
            break;
        case 4:
            dbManager.performComplexQuery();
            break;
        case 5:
            std::cout << "Enter your SELECT query (e.g., SELECT * FROM Rating;): \n> ";
            std::getline(std::cin, userSql);
            dbManager.performUserQuery(userSql);
            break;
        case 6:
            std::cout << "\nExiting the program. Goodbye!\n";
            break;
        default:
            std::cout << "\nInvalid choice. Please select an option from 1 to 6.\n";
            break;
        }
    } while (choice != 6); // Requirement 2: Continue displaying the menu until 'Exit'

    return 0;
}

