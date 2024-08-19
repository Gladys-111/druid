package com.alibaba.druid.sql.dialect.gaussdb.ast;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLPartitionBy;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.dialect.gaussdb.visitor.GaussDbASTVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public class GaussDbCreateTableStatement extends SQLCreateTableStatement implements GaussDbObject {
    protected SQLExpr engine;
    protected GaussDbDistributeBy distributeBy;
    protected SQLPartitionBy partitionBy;

    public GaussDbCreateTableStatement() {
        super(DbType.gaussdb);
    }

    public void setEngine(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.engine = x;
    }

    public SQLExpr getEngine() {
        return engine;
    }

    public void setDistributeBy(GaussDbDistributeBy distributeBy) {
        this.distributeBy = distributeBy;
    }

    public GaussDbDistributeBy getDistributeBy() {
        return distributeBy;
    }

    public void setPartitionBy(SQLPartitionBy partitionBy) {
        this.partitionBy = partitionBy;
    }

    public SQLPartitionBy getPartitionBy() {
        return partitionBy;
    }

    @Override
    public void accept0(SQLASTVisitor v) {
        if (v instanceof GaussDbASTVisitor) {
            GaussDbASTVisitor vv = (GaussDbASTVisitor) v;
            if (vv.visit(this)) {
                acceptChild(vv);
            }
            vv.endVisit(this);
            return;
        }

        if (v.visit(this)) {
            acceptChild(v);
        }
        v.endVisit(this);
    }

    @Override
    public void accept0(GaussDbASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.distributeBy);
        }
    }
}
