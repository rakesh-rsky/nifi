package org.apache.nifi.copilot.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class SessionStore {
    private static final Logger logger = LoggerFactory.getLogger(SessionStore.class);
    private static final Path DB_PATH = Path.of("nifi-copilot", "data", "copilot_sessions.db");
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SessionStore() {
        init();
    }

    private void init() {
        try {
            Files.createDirectories(DB_PATH.getParent());
            try (Connection c = getConnection()) {
                c.createStatement().execute("""
                        CREATE TABLE IF NOT EXISTS sessions (
                            process_group_id TEXT PRIMARY KEY,
                            data             TEXT NOT NULL,
                            updated_at       TEXT NOT NULL
                        )
                        """);
            }
        } catch (Exception e) {
            logger.warn("Failed to initialize session store: {}", e.getMessage());
        }
    }

    private Connection getConnection() throws Exception {
        return DriverManager.getConnection("jdbc:sqlite:" + DB_PATH.toAbsolutePath());
    }

    public Map<String, Object> load(final String processGroupId) {
        try (Connection c = getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT data FROM sessions WHERE process_group_id = ?")) {
            ps.setString(1, processGroupId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return objectMapper.readValue(rs.getString(1), new TypeReference<>() {
                });
            }
        } catch (Exception e) {
            logger.warn("Failed loading session {}: {}", processGroupId, e.getMessage());
            return null;
        }
    }

    public void save(final String processGroupId, final Object payload) {
        try (Connection c = getConnection();
             PreparedStatement ps = c.prepareStatement("""
                     INSERT INTO sessions(process_group_id, data, updated_at)
                     VALUES(?, ?, ?)
                     ON CONFLICT(process_group_id)
                     DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at
                     """)) {
            ps.setString(1, processGroupId);
            ps.setString(2, objectMapper.writeValueAsString(payload));
            ps.setString(3, Instant.now().toString());
            ps.executeUpdate();
        } catch (Exception e) {
            logger.warn("Failed saving session {}: {}", processGroupId, e.getMessage());
        }
    }

    public void delete(final String processGroupId) {
        try (Connection c = getConnection();
             PreparedStatement ps = c.prepareStatement("DELETE FROM sessions WHERE process_group_id = ?")) {
            ps.setString(1, processGroupId);
            ps.executeUpdate();
        } catch (Exception e) {
            logger.warn("Failed deleting session {}: {}", processGroupId, e.getMessage());
        }
    }
}
