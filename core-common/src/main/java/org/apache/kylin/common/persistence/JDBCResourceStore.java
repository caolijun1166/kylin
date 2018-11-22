/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.persistence;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.zip.DataFormatException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;

import com.google.common.base.Preconditions;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.DBUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCResourceStore extends PushdownResourceStore {

    private static Logger logger = LoggerFactory.getLogger(JDBCResourceStore.class);

    private static final String JDBC_SCHEME = "jdbc";

    private static final String META_TABLE_KEY = "META_TABLE_KEY";

    private static final String META_TABLE_TS = "META_TABLE_TS";

    private static final String META_TABLE_CONTENT = "META_TABLE_CONTENT";

    private static final byte[] GZIP = "GZIP".getBytes(Charset.forName("UTF-8"));

    // ============================================================================

    //检查jdbcconnectionmanager和ke比有没有改动
    private JDBCConnectionManager connectionManager;

    private String[] tableNames = new String[2];

    private String metadataIdentifier = null;

    // For test
    private long queriedSqlNum = 0;

    public JDBCResourceStore(KylinConfig kylinConfig) throws SQLException, IOException {
        super(kylinConfig);
        StorageURL metadataUrl = kylinConfig.getMetadataUrl();
        checkScheme(metadataUrl);
        this.metadataIdentifier = metadataUrl.getIdentifier();
        this.tableNames[0] = metadataIdentifier;
        this.tableNames[1] = metadataIdentifier + "_log";
        this.connectionManager = JDBCConnectionManager.getConnectionManager();
        for (int i = 0; i < tableNames.length; i++) {
            createTableIfNeeded(tableNames[i]);
        }
    }

    abstract static class SqlOperation {
        PreparedStatement pstat = null;
        ResultSet rs = null;

        abstract public void execute(final Connection connection) throws SQLException, IOException;
    }

    private void executeSql(SqlOperation operation) throws SQLException, IOException {
        Connection connection = null;
        try {
            connection = connectionManager.getConn();

            // set a low translation level for best performance
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            operation.execute(connection);
            queriedSqlNum++;
        } finally {
            DBUtils.closeQuietly(operation.rs);
            DBUtils.closeQuietly(operation.pstat);
            DBUtils.closeQuietly(connection);
        }
    }

    private boolean isDirectChild(String folderPrefix, String resPath) {
        assert resPath.startsWith(folderPrefix);
        int cut = resPath.indexOf('/', folderPrefix.length());
        return (cut < 0);
    }

    @Override
    protected boolean existsImpl(String resPath) throws IOException {
        try {
            RawResource resource = getResourceInteral(resPath, false, false);
            return (resource != null);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected RawResource getResourceImpl(String resPath) throws IOException {
        try {
            return getResourceInteral(resPath, true, true);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    RawResource getResourceInteral(final String resourcePath, final boolean fetchContent, final boolean fetchTimestamp)
            throws SQLException, IOException {
        logger.trace("getResource method. resourcePath : {} , fetchConetent : {} , fetch TS : {}", resourcePath,
                fetchContent, fetchTimestamp);

        final RawResource[] holder = new RawResource[1];
        JDBCResourceSQL sqls = getJDBCResourceSQL(getMetaTableName(resourcePath));
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                pstat = connection.prepareStatement(sqls.getKeyEqualSqlString(fetchContent, fetchTimestamp));
                pstat.setString(1, resourcePath);
                rs = pstat.executeQuery();
                if (rs.next()) {
                    holder[0] = rawResource(rs, fetchContent, fetchTimestamp);
                }
            }
        });

        return holder[0];
    }

    private RawResource rawResource(ResultSet rs, boolean fetchContent, boolean fetchTime) throws SQLException {
        String path = rs.getString(META_TABLE_KEY);
        long ts = fetchTime ? rs.getLong(META_TABLE_TS) : -1;

        if (fetchContent) {
            try {
                return new RawResource(path, ts, getInputStream(path, rs));
            } catch (IOException e) {
                return new RawResource(path, ts, e); // let the caller handle broken content
            } catch (SQLException e) {
                return new RawResource(path, ts, new IOException(e)); // let the caller handle broken content
            } catch (DataFormatException e) {
                return new RawResource(path, ts, new IOException(e)); // let the caller handle broken content
            }
        } else {
            return new RawResource(path, ts);
        }
    }

    private InputStream getInputStream(String resPath, ResultSet rs)
            throws SQLException, IOException, DataFormatException {
        if (rs == null) {
            return null;
        }

        Blob blob = rs.getBlob(META_TABLE_CONTENT);

        if (blob == null || blob.length() == 0) {
            return openPushdown(resPath); // empty bytes is pushdown indicator
        } else {
            byte[] bytes = blob.getBytes(1, (int) blob.length());
            byte[] dewrappedContent = dewrapContent(bytes);
            return new ByteArrayInputStream(dewrappedContent);
        }
    }

    private byte[] wrapContent(byte[] content) throws IOException {
        if (content == null || content.length == 0) {
            return content;
        }
        byte[] compressedContent = CompressionUtils.compress(content);
        byte[] wrappedContent = BytesUtil.mergeBytes(GZIP, compressedContent);
        return wrappedContent;
    }

    private byte[] dewrapContent(byte[] bytes) throws IOException, DataFormatException {
        Boolean isWrapped = false;
        if (bytes.length > GZIP.length) {
            isWrapped = true;
            for (int i = 0; i < GZIP.length; i++) {
                if (bytes[i] != GZIP[i]) {
                    isWrapped = false;
                    break;
                }
            }
        }
        if (isWrapped) {
            byte[] compressedContent = BytesUtil.subarray(bytes, GZIP.length, bytes.length);
            byte[] unwrappedContent = CompressionUtils.decompress(compressedContent);
            return unwrappedContent;
        }
        return bytes;
    }

    @Override
    protected long getResourceTimestampImpl(String resPath) throws IOException {
        try {
            RawResource resource = getResourceInteral(resPath, false, true);
            return resource == null ? 0 : resource.lastModified();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void putSmallResource(String resPath, ContentWriter content, long ts) throws IOException {
        try {
            putResourceInternal(resPath, content, ts);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    void putResourceInternal(final String resPath, final ContentWriter content, final long ts)
            throws SQLException, IOException {
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException, IOException {
                byte[] bytes = content.extractAllBytes();
                byte[] wrappedContent = wrapContent(bytes);
                synchronized (resPath.intern()) {
                    JDBCResourceSQL sqls = getJDBCResourceSQL(getMetaTableName(resPath));
                    boolean existing = existsImpl(resPath);
                    if (existing) {
                        pstat = connection.prepareStatement(sqls.getReplaceSql());
                        pstat.setLong(1, ts);
                        pstat.setBlob(2, new BufferedInputStream(new ByteArrayInputStream(wrappedContent)));
                        pstat.setString(3, resPath);
                    } else {
                        pstat = connection.prepareStatement(sqls.getInsertSql());
                        pstat.setString(1, resPath);
                        pstat.setLong(2, ts);
                        pstat.setBlob(3, new BufferedInputStream(new ByteArrayInputStream(wrappedContent)));
                    }

                    //如果metadata过大，则将元数据下压到pushdown
                    if (isContentOverflow(wrappedContent, resPath)) {
                        logger.debug("Overflow! resource path: {}, content size: {}, timeStamp: {}", resPath,
                                bytes.length, ts);
                        if (existing) {
                            pstat.setNull(2, Types.BLOB);
                        } else {
                            pstat.setNull(3, Types.BLOB);
                        }

                        RollbackablePushdown pushdown = writePushdown(resPath, ContentWriter.create(bytes));
                        try {
                            int result = pstat.executeUpdate();
                            if (result != 1)
                                throw new SQLException();
                        } catch (Throwable ex) {
                            pushdown.rollback();
                            throw ex;
                        } finally {
                            pushdown.close();
                        }
                    } else {
                        pstat.executeUpdate();
                    }
                }
            }
        });
    }

    private boolean isContentOverflow(byte[] content, String resPath) throws SQLException {
        if (kylinConfig.isJsonAlwaysSmallCell() && isJsonMetadata(resPath)) {

            int smallCellMetadataWarningThreshold = kylinConfig.getSmallCellMetadataWarningThreshold();
            int smallCellMetadataErrorThreshold = kylinConfig.getSmallCellMetadataErrorThreshold();

            if (content.length > smallCellMetadataWarningThreshold) {
                logger.warn(
                        "A JSON metadata entry's size is not supposed to exceed kap.metadata.jdbc.small-cell-meta-size-warning-threshold("
                                + smallCellMetadataWarningThreshold + "), resPath: " + resPath + ", actual size: "
                                + content.length);
            }
            if (content.length > smallCellMetadataErrorThreshold) {
                throw new SQLException(new IllegalArgumentException(
                        "A JSON metadata entry's size is not supposed to exceed kap.metadata.jdbc.small-cell-meta-size-error-threshold("
                                + smallCellMetadataErrorThreshold + "), resPath: " + resPath + ", actual size: "
                                + content.length));
            }

            return false;
        }

        int maxSize = kylinConfig.getJdbcResourceStoreMaxCellSize();
        if (content.length > maxSize)
            return true;
        else
            return false;
    }

    @Override
    protected long checkAndPutResourceImpl(String resPath, byte[] content, long oldTS, long newTS)
            throws IOException, WriteConflictException {
        try {
            checkAndPutResourceInternal(resPath, content, oldTS, newTS);
            return newTS;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    void checkAndPutResourceInternal(final String resPath, final byte[] content, final long oldTS, final long newTS)
            throws SQLException, IOException, WriteConflictException {
        logger.trace(
                "execute checkAndPutResource method. resPath : {} , oldTs : {} , newTs : {} , content null ? : {} ",
                resPath, oldTS, newTS, content == null);
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException, IOException {
                byte[] wrappedContent = wrapContent(content);
                synchronized (resPath.intern()) {
                    JDBCResourceSQL sqls = getJDBCResourceSQL(getMetaTableName(resPath));
                    if (!existsImpl(resPath)) {
                        if (oldTS != 0) {
                            throw new IllegalStateException(
                                    "For not exist file. OldTS have to be 0. but Actual oldTS is : " + oldTS);
                        }
                        if (isContentOverflow(wrappedContent, resPath)) {
                            logger.debug("Overflow! resource path: {}, content size: {}", resPath, content.length);
                            pstat = connection.prepareStatement(sqls.getInsertSqlWithoutContent());
                            pstat.setString(1, resPath);
                            pstat.setLong(2, newTS);
                            RollbackablePushdown pushdown = writePushdown(resPath, ContentWriter.create(content));
                            try {
                                int result = pstat.executeUpdate();
                                if (result != 1)
                                    throw new SQLException();
                            } catch (Throwable e) {
                                pushdown.rollback();
                                throw e;
                            } finally {
                                pushdown.close();
                            }
                        } else {
                            pstat = connection.prepareStatement(sqls.getInsertSql());
                            pstat.setString(1, resPath);
                            pstat.setLong(2, newTS);
                            pstat.setBlob(3, new BufferedInputStream(new ByteArrayInputStream(wrappedContent)));
                            pstat.executeUpdate();
                        }
                    } else {
                        // Note the checkAndPut trick:
                        // update {0} set {1}=? where {2}=? and {3}=?
                        pstat = connection.prepareStatement(sqls.getUpdateSqlWithoutContent());
                        pstat.setLong(1, newTS);
                        pstat.setString(2, resPath);
                        pstat.setLong(3, oldTS);
                        int result = pstat.executeUpdate();
                        if (result != 1) {
                            long realTime = getResourceTimestamp(resPath);
                            throw new WriteConflictException("Overwriting conflict " + resPath + ", expect old TS "
                                    + oldTS + ", but it is " + realTime);
                        }
                        PreparedStatement pstat2 = null;
                        try {
                            // "update {0} set {1}=? where {3}=?"
                            pstat2 = connection.prepareStatement(sqls.getUpdateContentSql());
                            if (isContentOverflow(wrappedContent, resPath)) {
                                logger.debug("Overflow! resource path: {}, content size: {}", resPath, content.length);
                                pstat2.setNull(1, Types.BLOB);
                                pstat2.setString(2, resPath);
                                RollbackablePushdown pushdown = writePushdown(resPath, ContentWriter.create(content));
                                try {
                                    int result2 = pstat2.executeUpdate();
                                    if (result2 != 1)
                                        throw new SQLException();
                                } catch (Throwable e) {
                                    pushdown.rollback();
                                    throw e;
                                } finally {
                                    pushdown.close();
                                }
                            } else {
                                pstat2.setBinaryStream(1,
                                        new BufferedInputStream(new ByteArrayInputStream(wrappedContent)));
                                pstat2.setString(2, resPath);
                                pstat2.executeUpdate();
                            }
                        } finally {
                            JDBCConnectionManager.closeQuietly(pstat2);
                        }
                    }
                }
            }
        });
    }

    @Override
    protected void deleteResourceImpl(String resPath) throws IOException {
        try {
            boolean skipHdfs = isJsonMetadata(resPath);
            JDBCResourceSQL sqls = getJDBCResourceSQL(getMetaTableName(resPath));
            executeSql(new SqlOperation() {
                @Override
                public void execute(Connection connection) throws SQLException {
                        pstat = connection.prepareStatement(sqls.getDeletePstatSql());
                        pstat.setString(1, resPath);
                        pstat.executeUpdate();
                }
            });

            if (!skipHdfs) {
                try {
                    deletePushdown(resPath);
                } catch (Throwable e) {
                    throw new SQLException(e);
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected String getReadableResourcePathImpl(String resPath) {
        return metadataIdentifier + "(key='" + resPath + "')@" + kylinConfig.getMetadataUrl();
    }

    @Override
    protected void visitFolderImpl(final String folderPath, final boolean recursive, final VisitFilter filter,
                                   final boolean loadContent, final Visitor visitor) throws IOException {

        try {
            executeSql(new SqlOperation() {
                @Override
                public void execute(Connection connection) throws SQLException {
                    String folderPrefix = folderPath.endsWith("/") ? folderPath : folderPath + "/";
                    String lookForPrefix = folderPrefix;
                    if (filter.hasPathPrefixFilter()) {
                        Preconditions.checkArgument(filter.pathPrefix.startsWith(folderPrefix));
                        lookForPrefix = filter.pathPrefix;
                    }
                        JDBCResourceSQL sqls = getJDBCResourceSQL(getMetaTableName(folderPath));
                        String sql = sqls.getAllResourceSqlString(loadContent);
                        pstat = connection.prepareStatement(sql);
                        pstat.setLong(1, filter.lastModStart);
                        pstat.setLong(2, filter.lastModEndExclusive);
                        // '_' is LIKE wild char, need escape
                        pstat.setString(3, lookForPrefix.replace("_", "#_") + "%");
                        rs = pstat.executeQuery();
                        while (rs.next()) {
                            String resPath = rs.getString(META_TABLE_KEY);
                            if (resPath.equals(folderPath))
                                continue; // the folder itself exists as a resource? ignore..

                            if (recursive || isDirectChild(folderPrefix, resPath)) {
                                RawResource raw = rawResource(rs, loadContent, true);
                                try {
                                    visitor.visit(raw);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                } finally {
                                    raw.close();
                                }
                            }
                        }
                }
            });
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private void createTableIfNeeded(final String tableName) throws SQLException, IOException {
        JDBCResourceSQL sqls = getJDBCResourceSQL(tableName);
        executeSql(new SqlOperation() {
            @Override
            public void execute(Connection connection) throws SQLException {
                if (checkTableExists(tableName, connection)) {
                    logger.info("Table [{}] already exists", tableName);
                    return;
                }

                String createIfNeededSql = sqls.getCreateIfNeededSql(tableName);
                logger.info("Creating table: " + createIfNeededSql);
                pstat = connection.prepareStatement(createIfNeededSql);
                pstat.executeUpdate();

                try {
                    String indexName = "IDX_" + META_TABLE_TS;
                    String createIndexSql = sqls.getCreateIndexSql(indexName, tableName, META_TABLE_TS);
                    logger.info("Creating index: " + createIndexSql);
                    pstat = connection.prepareStatement(createIndexSql);
                    pstat.executeUpdate();
                } catch (SQLException ex) {
                    logger.error("Failed to create index on " + META_TABLE_TS, ex);
                }
            }

            private boolean checkTableExists(final String tableName, final Connection connection) throws SQLException {
                PreparedStatement ps = null;
                ResultSet rs = null;
                JDBCResourceSQL sqls = getJDBCResourceSQL(tableName);
                try {
                    String checkTableExistsSql = sqls.getCheckTableExistsSql(tableName);
                    ps = connection.prepareStatement(checkTableExistsSql);
                    rs = ps.executeQuery();
                    while (rs.next()) {
                        // use equalsIgnoreCase() as some RDBMS is case insensitive
                        if (tableName.equalsIgnoreCase(rs.getString(1))) {
                            return true;
                        }
                    }
                } finally {
                    DBUtils.closeQuietly(rs);
                    DBUtils.closeQuietly(ps);
                }

                return false;
            }
        });
    }

    @Override
    protected String pushdownRootPath() {
        String metastoreBigCellHdfsDirectory = kylinConfig.getMetastoreBigCellHdfsDirectory();
        if (metastoreBigCellHdfsDirectory.endsWith("/"))
            return metastoreBigCellHdfsDirectory + "resources-jdbc";
        else
            return metastoreBigCellHdfsDirectory + "/" + "resources-jdbc";
    }

    public long getQueriedSqlNum() {
        return queriedSqlNum;
    }


    private boolean isJsonMetadata(String resourcePath) {
        String trim = resourcePath.trim();
        return trim.endsWith(".json") || trim.startsWith(ResourceStore.EXECUTE_RESOURCE_ROOT)
                || trim.startsWith(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT);
    }

    public static void checkScheme(StorageURL url) {
        Preconditions.checkState(JDBC_SCHEME.equals(url.getScheme()));
    }

    public String getMetaTableName(String resPath) {
        if (isRootPath(resPath)) {
            throw new IllegalArgumentException("Not supported");
        }

        if (resPath.startsWith(ResourceStore.BAD_QUERY_RESOURCE_ROOT)
                || resPath.startsWith(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT)
                || resPath.startsWith(ResourceStore.TEMP_STATMENT_RESOURCE_ROOT)) {
            return tableNames[1];
        } else {
            return tableNames[0];
        }
    }

    public JDBCResourceSQL getJDBCResourceSQL(String metaTableName) {
        return new JDBCResourceSQL(kylinConfig.getMetadataDialect(), metaTableName, META_TABLE_KEY, META_TABLE_TS,
                META_TABLE_CONTENT);
    }

    public boolean isRootPath(String path) {
        return "/".equals(path);
    }
}