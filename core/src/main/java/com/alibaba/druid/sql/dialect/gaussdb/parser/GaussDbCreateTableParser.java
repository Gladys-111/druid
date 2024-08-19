package com.alibaba.druid.sql.dialect.gaussdb.parser;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLPartitionBy;
import com.alibaba.druid.sql.ast.SQLPartitionByHash;
import com.alibaba.druid.sql.ast.SQLPartitionByList;
import com.alibaba.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.dialect.gaussdb.ast.GaussDbCreateTableStatement;
import com.alibaba.druid.sql.dialect.gaussdb.ast.GaussDbDistributeBy;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLCreateTableParser;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.druid.util.FnvHash;

public class GaussDbCreateTableParser extends SQLCreateTableParser {
    public GaussDbCreateTableParser(String sql) {
        super(new GaussDbExprParser(sql));
    }

    protected SQLCreateTableStatement newCreateStatement() {
        return new GaussDbCreateTableStatement();
    }

    @Override
    public GaussDbExprParser getExprParser() {
        return (GaussDbExprParser) exprParser;
    }

    public GaussDbCreateTableParser(SQLExprParser exprParser) {
        super(exprParser);
        dbType = DbType.gaussdb;
    }

    protected void parseCreateTableRest(SQLCreateTableStatement stmt) {
        GaussDbCreateTableStatement gdStmt = (GaussDbCreateTableStatement) stmt;
        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLAssignItem sqlAssignItem = new SQLAssignItem(new SQLIdentifierExpr("COMMENT"), this.exprParser.expr());
            gdStmt.getTableOptions().add(sqlAssignItem);
        }
        if (lexer.identifierEquals(FnvHash.Constants.AUTO_INCREMENT)) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLAssignItem sqlAssignItem = new SQLAssignItem(new SQLIdentifierExpr("AUTO_INCREMENT"), this.exprParser.integerExpr());
            gdStmt.getTableOptions().add(sqlAssignItem);
        }
        if (lexer.token() == Token.DEFAULT) {
            lexer.nextToken();
        }
        if (lexer.identifierEquals(FnvHash.Constants.CHARSET)) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLAssignItem sqlAssignItem = new SQLAssignItem(new SQLIdentifierExpr("CHARSET"), this.exprParser.expr());
            gdStmt.getTableOptions().add(sqlAssignItem);
        } else if (lexer.identifierEquals(FnvHash.Constants.CHARACTER)) {
            lexer.nextToken();
            accept(Token.SET);
            SQLAssignItem sqlAssignItem = new SQLAssignItem(new SQLIdentifierExpr("CHARSET"), this.exprParser.expr());
            gdStmt.getTableOptions().add(sqlAssignItem);
        }
        if (lexer.identifierEquals(FnvHash.Constants.COLLATE)) {
            lexer.nextToken();
            if (lexer.token() == Token.EQ) {
                lexer.nextToken();
            }
            SQLAssignItem sqlAssignItem = new SQLAssignItem(new SQLIdentifierExpr("COLLATE"), this.exprParser.expr());
            gdStmt.getTableOptions().add(sqlAssignItem);
        }
        if (lexer.identifierEquals(FnvHash.Constants.ENGINE)) {
            lexer.nextToken();
            accept(Token.EQ);
            gdStmt.setEngine(
                    this.exprParser.expr()
            );
        }
        if (lexer.token() == Token.WITH) {
            lexer.nextToken();
            accept(Token.LPAREN);
            parseAssignItems(gdStmt.getTableOptions(), gdStmt, false);
            accept(Token.RPAREN);
        }
        //Distribute
        GaussDbDistributeBy distributeByClause = parseDistributeBy();
        gdStmt.setDistributeBy(distributeByClause);
        SQLPartitionBy partitionClause = parsePartitionBy();
        gdStmt.setPartitionBy(partitionClause);
    }

    public SQLPartitionBy parsePartitionBy() {
        if (lexer.nextIf(Token.PARTITION)) {
            accept(Token.BY);
            if (lexer.nextIfIdentifier(FnvHash.Constants.HASH)) {
                SQLPartitionBy hashPartition = new SQLPartitionByHash();
                if (lexer.nextIf(Token.LPAREN)) {
                    // e.g. partition by hash(id,name) partitions 16
                    // TODO: 'partition by hash(id) partitions 4, hash(name) partitions 4' not supported yet
                    if (lexer.token() != Token.IDENTIFIER) {
                        throw new ParserException("expect identifier. " + lexer.info());
                    }
                    for (; ; ) {
                        hashPartition.addColumn(this.exprParser.name());
                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }
                    accept(Token.RPAREN);
                    return hashPartition;
                }
            } else if (lexer.nextIfIdentifier(FnvHash.Constants.RANGE)) {
                return partitionByRange();
            } else if (lexer.nextIfIdentifier(FnvHash.Constants.LIST)) {
                return partitionByList();
            }
        }
        return null;
    }

    private SQLPartitionByRange partitionByRange() {
        SQLPartitionByRange rangePartition = new SQLPartitionByRange();
        accept(Token.LPAREN);
        for (; ; ) {
            rangePartition.addColumn(this.exprParser.name());
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }
        accept(Token.RPAREN);
        accept(Token.LPAREN);
        for (; ; ) {
            rangePartition.addPartition(this.getExprParser().parsePartition());
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }
        accept(Token.RPAREN);
        return rangePartition;
    }

    private SQLPartitionByList partitionByList() {
        SQLPartitionByList listPartition = new SQLPartitionByList();
        accept(Token.LPAREN);
        for (; ; ) {
            listPartition.addColumn(this.exprParser.name());
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }
        accept(Token.RPAREN);
        accept(Token.LPAREN);
        for (; ; ) {
            listPartition.addPartition(this.getExprParser().parsePartition());
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }
        accept(Token.RPAREN);
        return listPartition;
    }

    public GaussDbDistributeBy parseDistributeBy() {
        if (lexer.token() == Token.DISTRIBUTE) {
            lexer.nextToken();
            accept(Token.BY);
            GaussDbDistributeBy distributeBy = new GaussDbDistributeBy();
            if (lexer.identifierEquals(FnvHash.Constants.HASH)) {
                distributeBy.setType(this.exprParser.name());
                if (lexer.nextIf(Token.LPAREN)) {
                    if (lexer.token() != Token.IDENTIFIER) {
                        throw new ParserException("expect identifier. " + lexer.info());
                    }
                    for (; ; ) {
                        distributeBy.addColumn(this.exprParser.name());
                        if (lexer.token() == Token.COMMA) {
                            lexer.nextToken();
                            continue;
                        }
                        break;
                    }
                    accept(Token.RPAREN);
//                    acceptIdentifier("PARTITIONS");
//                    hashPartition.setPartitionsCount(acceptInteger());
                    return distributeBy;
                }
            } else if (lexer.identifierEquals(FnvHash.Constants.RANGE)) {
                distributeBy.setType(this.exprParser.name());
                return distributitionByContent(distributeBy);
            } else if (lexer.identifierEquals(FnvHash.Constants.LIST)) {
                distributeBy.setType(this.exprParser.name());
                return distributitionByContent(distributeBy);
            }
        }
        return null;
    }

    public GaussDbDistributeBy distributitionByContent(GaussDbDistributeBy distributeBy) {
        accept(Token.LPAREN);
        for (; ; ) {
            distributeBy.addColumn(this.exprParser.name());
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }
        accept(Token.RPAREN);
        accept(Token.LPAREN);
        for (; ; ) {
            distributeBy.addDistribution(this.getExprParser().parseDistribution());
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }
        accept(Token.RPAREN);
        return distributeBy;
    }
}
