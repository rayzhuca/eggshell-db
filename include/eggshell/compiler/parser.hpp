#pragma once

#include <string>
#include <vector>

enum class ASTNodeType { select, insert };

struct ASTNode {
    ASTNodeType type;

    ASTNode(ASTNodeType type);
};

// Define the AST node for SELECT statement
struct SelectNode : ASTNode {
    std::vector<std::string> columns;
    std::string table;

    SelectNode(ASTNodeType type);
};

// Define the AST node for INSERT statement
struct InsertNode : ASTNode {
    std::string table;
    std::vector<std::string> values;

    InsertNode(ASTNodeType type);
};

ASTNode parseSQL(const std::string& query);