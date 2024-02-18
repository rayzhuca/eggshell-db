#include "compiler/parser.hpp"

#include <iostream>
#include <string>
#include <vector>

std::string to_upper_case(const std::string& str) {
    std::string new_str = str;
    std::transform(str.begin(), str.end(), new_str.begin(), ::toupper);
    std::cout << new_str << "\n";
    return new_str;
}

ASTNode::ASTNode(ASTNodeType type) : type{type} {
}

SelectNode::SelectNode(ASTNodeType type) : ASTNode{type} {
}

InsertNode::InsertNode(ASTNodeType type) : ASTNode{type} {
}

// Parse the SQL query and return the AST
ASTNode parseSQL(const std::string& query) {
    // The query is in the format:
    // SELECT column1, column2 FROM table_name;
    // INSERT INTO table_name VALUES (value1, value2, ...);

    // Parsing logic for SELECT statement
    if (to_upper_case(query.substr(0, 12)) == "SELECT INTO") {
        SelectNode selectNode{ASTNodeType::select};

        // Extract columns
        size_t fromPos = query.find("FROM");
        std::string columnsStr = query.substr(7, fromPos - 7);
        size_t commaPos = 0;
        while ((commaPos = columnsStr.find(",")) != std::string::npos) {
            std::string column = columnsStr.substr(0, commaPos);
            selectNode.columns.push_back(column);
            columnsStr.erase(0, commaPos + 1);
        }
        selectNode.columns.push_back(columnsStr);

        // Extract table
        size_t tablePos = query.find("FROM") + 5;
        size_t semicolonPos = query.find(";");
        selectNode.table = query.substr(tablePos, semicolonPos - tablePos);

        return selectNode;

        // std::cout << "Parsed SELECT statement AST:" << std::endl;
        // std::cout << "Columns: ";
        // for (const auto& column : selectNode.columns) {
        //     std::cout << column << " ";
        // }
        // std::cout << std::endl;
        // std::cout << "Table: " << selectNode.table << std::endl;
    }

    // Parsing logic for INSERT statement
    else if (to_upper_case(query.substr(0, 6)) == "INSERT") {
        InsertNode insertNode{ASTNodeType::insert};

        // Extract table
        size_t intoPos = query.find("INTO") + 5;
        size_t valuesPos = query.find("VALUES");
        insertNode.table = query.substr(intoPos, valuesPos - intoPos);

        // Extract values
        size_t openParenPos = query.find("(");
        size_t closeParenPos = query.find(")");
        std::string valuesStr =
            query.substr(openParenPos + 1, closeParenPos - openParenPos - 1);
        size_t commaPos = 0;
        while ((commaPos = valuesStr.find(",")) != std::string::npos) {
            std::string value = valuesStr.substr(0, commaPos);
            insertNode.values.push_back(value);
            valuesStr.erase(0, commaPos + 1);
        }
        insertNode.values.push_back(valuesStr);

        return insertNode;

        // std::cout << "Parsed INSERT statement AST:" << std::endl;
        // std::cout << "Table: " << insertNode.table << std::endl;
        // std::cout << "Values: ";
        // for (const auto& value : insertNode.values) {
        //     std::cout << value << " ";
        // }
        // std::cout << std::endl;
    }

    // TODO: Handle other SQL statements if needed

    else {
        std::cout << "Unsupported SQL statement" << std::endl;
        exit(EXIT_FAILURE);
    }
}