/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.druid.sql.dialect.hive.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.hive.ast.HiveInputOutputFormat;
import com.alibaba.druid.sql.dialect.hive.stmt.HiveCreateTableStatement;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.FnvHash;

public class HiveCreateTableParser extends SQLCreateTableParser {
    public HiveCreateTableParser(SQLExprParser exprParser) {
        super(exprParser);
    }

    public HiveCreateTableParser(Lexer lexer) {
        super(new HiveExprParser(lexer));
    }

    protected void createTableBefore(SQLCreateTableStatement stmt) {
        if (lexer.nextIfIdentifier(FnvHash.Constants.EXTERNAL)) {
            stmt.setExternal(true);
        }

        if (lexer.nextIfIdentifier(FnvHash.Constants.TEMPORARY)) {
            stmt.setTemporary(true);
        }

        if (lexer.nextIfIdentifier("TRANSACTIONAL")) {
            stmt.config(SQLCreateTableStatement.Feature.Transactional);
        }
    }

    protected void createTableBody(SQLCreateTableStatement stmt) {
        if (lexer.token() == Token.LPAREN) {
            lexer.nextToken();

            for (; ; ) {
                Token token = lexer.token();
                if (token == Token.IDENTIFIER //
                        || token == Token.LITERAL_ALIAS) {
                    SQLColumnDefinition column = this.exprParser.parseColumn();
                    stmt.getTableElementList().add(column);
                } else if (token == Token.PRIMARY //
                        || token == Token.UNIQUE //
                        || token == Token.CHECK //
                        || token == Token.CONSTRAINT
                        || token == Token.FOREIGN) {
                    SQLConstraint constraint = this.exprParser.parseConstaint();
                    constraint.setParent(stmt);
                    stmt.getTableElementList().add((SQLTableElement) constraint);
                } else if (token == Token.TABLESPACE) {
                    throw new ParserException("TODO " + lexer.info());
                } else {
                    SQLColumnDefinition column = this.exprParser.parseColumn();
                    stmt.getTableElementList().add(column);
                }

                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();

                    if (lexer.token() == Token.RPAREN) { // compatible for sql server
                        break;
                    }
                    continue;
                }

                break;
            }

            accept(Token.RPAREN);
        } else if (lexer.token() == Token.LIKE) {
            parseLike((HiveCreateTableStatement) stmt);
        }
    }

    protected void createTableQuery(SQLCreateTableStatement stmt) {
        if (lexer.token() == Token.SELECT || lexer.token() == Token.AS) {
            if (lexer.token() == Token.AS) {
                lexer.nextToken();
            }
            SQLSelect select = this.createSQLSelectParser().select();
            stmt.setSelect(select);
        }
    }

    protected void parseCreateTableRest(SQLCreateTableStatement createTable) {
        HiveCreateTableStatement stmt = (HiveCreateTableStatement) createTable;
        if (lexer.nextIfIdentifier(FnvHash.Constants.ENGINE)) {
            // skip engine=xxx
            accept(Token.EQ);
            lexer.nextToken();
        }

        if (lexer.nextIfIdentifier(FnvHash.Constants.CHARSET)) {
            // skip charset = xxx
            accept(Token.EQ);
            lexer.nextToken();
        }

        if (lexer.identifierEquals(FnvHash.Constants.USING) || lexer.token() == Token.USING) {
            lexer.nextToken();
            SQLExpr using = this.exprParser.expr();
            stmt.setUsing(using);
        }

        if (lexer.nextIfIdentifier(FnvHash.Constants.OPTIONS)) {
            accept(Token.LPAREN);
            parseAssignItems(stmt.getTableOptions(), stmt, false);
            accept(Token.RPAREN);
        }

        if (lexer.nextIf(Token.COMMENT)) {
            SQLExpr comment = this.exprParser.expr();
            stmt.setComment(comment);
        }

        if (lexer.nextIfIdentifier(FnvHash.Constants.MAPPED)) {
            accept(Token.BY);
            this.exprParser.parseAssignItem(stmt.getMappedBy(), stmt);
        }

        if (lexer.nextIf(Token.PARTITIONED)) {
            accept(Token.BY);
            accept(Token.LPAREN);

            for (; ; ) {
                if (lexer.token() != Token.IDENTIFIER) {
                    throw new ParserException("expect identifier. " + lexer.info());
                }

                SQLColumnDefinition column = this.exprParser.parseColumn();
                stmt.addPartitionColumn(column);

                if (lexer.isKeepComments() && lexer.hasComment()) {
                    column.addAfterComment(lexer.readAndResetComments());
                }

                if (lexer.token() != Token.COMMA) {
                    break;
                } else {
                    lexer.nextToken();
                    if (lexer.isKeepComments() && lexer.hasComment()) {
                        column.addAfterComment(lexer.readAndResetComments());
                    }
                }
            }

            accept(Token.RPAREN);
        }

        if (lexer.nextIfIdentifier(FnvHash.Constants.CLUSTERED)) {
            accept(Token.BY);
            accept(Token.LPAREN);
            for (; ; ) {
                SQLSelectOrderByItem item = this.exprParser.parseSelectOrderByItem();
                stmt.addClusteredByItem(item);
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }
            accept(Token.RPAREN);
        }

        if (lexer.nextIfIdentifier(FnvHash.Constants.SKEWED)) {
            accept(Token.BY);
            accept(Token.LPAREN);
            this.exprParser.exprList(stmt.getSkewedBy(), stmt);
            accept(Token.RPAREN);
            accept(Token.ON);
            accept(Token.LPAREN);
            for (; ; ) {
                if (lexer.token() == Token.LPAREN) {
                    SQLListExpr list = new SQLListExpr();
                    lexer.nextToken();
                    this.exprParser.exprList(list.getItems(), list);
                    accept(Token.RPAREN);
                    stmt.addSkewedByOn(list);
                } else {
                    SQLExpr expr = this.exprParser.expr();
                    stmt.addSkewedByOn(expr);
                }
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }
            accept(Token.RPAREN);
        }

        if (lexer.identifierEquals(FnvHash.Constants.SORTED)) {
            parseSortedBy(stmt);
        }

        if (lexer.token() == Token.ROW
                || lexer.identifierEquals(FnvHash.Constants.ROW)) {
            parseRowFormat(stmt);
        }

        if (lexer.identifierEquals(FnvHash.Constants.SORTED)) {
            parseSortedBy(stmt);
        }

        if (stmt.getClusteredBy().size() > 0 || stmt.getSortedBy().size() > 0) {
            accept(Token.INTO);
            if (lexer.token() == Token.LITERAL_INT) {
                stmt.setBuckets(lexer.integerValue().intValue());
                lexer.nextToken();
            } else {
                throw new ParserException("into buckets must be integer. " + lexer.info());
            }
            acceptIdentifier("BUCKETS");
        }

        if (lexer.token() == Token.ROW
                || lexer.identifierEquals(FnvHash.Constants.ROW)) {
            parseRowFormat(stmt);
        }

        if (Token.LBRACKET.equals(lexer.token())) {
            stmt.setLbracketUse(true);
            lexer.nextToken();
        }
        if (lexer.identifierEquals(FnvHash.Constants.STORED)) {
            lexer.nextToken();
            if (lexer.token() == Token.BY) {
                accept(Token.BY);
                SQLName name = this.exprParser.name();
                stmt.setStoredBy(name);
            } else {
                accept(Token.AS);

                if (lexer.identifierEquals(FnvHash.Constants.INPUTFORMAT)) {
                    HiveInputOutputFormat format = new HiveInputOutputFormat();
                    lexer.nextToken();
                    format.setInput(this.exprParser.primary());

                    if (lexer.identifierEquals(FnvHash.Constants.OUTPUTFORMAT)) {
                        lexer.nextToken();
                        format.setOutput(this.exprParser.primary());
                    }
                    stmt.setStoredAs(format);
                } else {
                    SQLName name = this.exprParser.name();
                    stmt.setStoredAs(name);
                }
            }
        }

        if (Token.RBRACKET.equals(lexer.token())) {
            stmt.setRbracketUse(true);
            lexer.nextToken();
        }
        if (lexer.identifierEquals(FnvHash.Constants.LOCATION)) {
            lexer.nextToken();
            SQLExpr location = this.exprParser.primary();
            stmt.setLocation(location);
        }

        if (lexer.token() == Token.LIKE) {
            parseLike(stmt);
        }

        if (lexer.identifierEquals(FnvHash.Constants.TBLPROPERTIES)) {
            parseOptions(stmt);
        }

        if (lexer.identifierEquals(FnvHash.Constants.META)) {
            lexer.nextToken();
            acceptIdentifier("LIFECYCLE");
            stmt.setLifeCycle(this.exprParser.primary());
        }

        createTableQuery(stmt);

        if (lexer.token() == Token.LIKE) {
            lexer.nextToken();
            Lexer.SavePoint mark = lexer.mark();
            if (lexer.token() == Token.SELECT) {
                stmt.setLikeQuery(true);
                SQLSelect select = this.createSQLSelectParser().select();
                stmt.setSelect(select);
            } else {
                lexer.reset(mark);

                if (lexer.identifierEquals(FnvHash.Constants.MAPPING)) {
                    SQLExpr like = this.exprParser.primary();
                    stmt.setLike(new SQLExprTableSource(like));
                } else {
                    SQLName name = this.exprParser.name();
                    stmt.setLike(name);
                }
            }
        }

        if (lexer.token() == Token.COMMENT) {
            lexer.nextToken();
            SQLExpr comment = this.exprParser.expr();
            stmt.setComment(comment);
        }

        if (lexer.identifierEquals(FnvHash.Constants.USING) || lexer.token() == Token.USING) {
            lexer.nextToken();
            SQLExpr using = this.exprParser.expr();
            stmt.setUsing(using);
        }

        if (lexer.identifierEquals(FnvHash.Constants.TBLPROPERTIES)) {
            lexer.nextToken();
            accept(Token.LPAREN);
            parseAssignItems(stmt.getTableOptions(), stmt, false);
            accept(Token.RPAREN);
        }
    }

    protected void parseOptions(SQLCreateTableStatement stmt) {
        lexer.nextToken();
        accept(Token.LPAREN);

        for (; ; ) {
            String name = lexer.stringVal();
            lexer.nextToken();
            if (lexer.token() == Token.DOT) {
                lexer.nextToken();
                name += "." + lexer.stringVal();
                lexer.nextToken();
            }

            accept(Token.EQ);
            SQLExpr value = this.exprParser.primary();
            stmt.addOption(name, value);
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                if (lexer.token() == Token.RPAREN) {
                    break;
                }
                continue;
            }
            break;
        }

        accept(Token.RPAREN);
    }

    protected void parseLike(HiveCreateTableStatement stmt) {
        lexer.nextToken();

        if (lexer.identifierEquals(FnvHash.Constants.MAPPING)) {
            SQLExpr like = this.exprParser.primary();
            stmt.setLike(new SQLExprTableSource(like));
        } else if (lexer.token() == Token.SELECT || lexer.token() == Token.LPAREN) {
            SQLSelect select = this.createSQLSelectParser().select();
            stmt.setLikeQuery(true);
            stmt.setSelect(select);
        } else {
            SQLName name = this.exprParser.name();
            stmt.setLike(name);
        }
    }

    private void parseSortedBy(HiveCreateTableStatement stmt) {
        lexer.nextToken();
        accept(Token.BY);
        accept(Token.LPAREN);
        for (; ; ) {
            SQLSelectOrderByItem item = this.exprParser.parseSelectOrderByItem();
            stmt.addSortedByItem(item);
            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }
            break;
        }
        accept(Token.RPAREN);
    }

    private void parseRowFormat(HiveCreateTableStatement stmt) {
        SQLExternalRecordFormat format = this.getExprParser().parseRowFormat();
        stmt.setRowFormat(format);

        if (lexer.token() == Token.WITH) {
            lexer.nextToken();
            acceptIdentifier("SERDEPROPERTIES");

            accept(Token.LPAREN);

            for (; ; ) {
                String name = lexer.stringVal();
                lexer.nextToken();
                accept(Token.EQ);
                SQLExpr value = this.exprParser.primary();
                stmt.getSerdeProperties().put(name, value);
                if (lexer.token() == Token.COMMA) {
                    lexer.nextToken();
                    continue;
                }
                break;
            }

            accept(Token.RPAREN);
        }
    }

    @Override
    public HiveExprParser getExprParser() {
        return (HiveExprParser) exprParser;
    }

    protected HiveCreateTableStatement newCreateStatement() {
        return new HiveCreateTableStatement();
    }

    public SQLSelectParser createSQLSelectParser() {
        return new HiveSelectParser(this.exprParser, selectListCache);
    }
}
