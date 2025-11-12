/*******************************************************************************
 * Copyright (C) 2020, exense GmbH
 *
 * This file is part of STEP
 *
 * STEP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * STEP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with STEP.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package step.framework.server.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.core.accessors.AbstractOrganizableObject;
import step.core.accessors.AbstractUser;
import step.core.objectenricher.ObjectEnricher;
import step.framework.server.AbstractServices;
import step.framework.server.Session;

import java.util.*;

public class AuditLogger {
    public static final String CONF_LOG_ENTITY_MODIFICATIONS = "auditLog.logEntityModifications";

    private static final Logger auditLogger = LoggerFactory.getLogger("AuditLogger");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static boolean entityModificationsLoggingEnabled = false;

    public static boolean isEntityModificationsLoggingEnabled() {
        return entityModificationsLoggingEnabled;
    }

    public static void setEntityModificationsLoggingEnabled(boolean enabled) {
        entityModificationsLoggingEnabled = enabled;
    }


    public static void trace(HttpServletRequest req, int status) {
        if (auditLogger.isTraceEnabled()) {
            String log = getLogMessage(req, status);
            auditLogger.trace(log);
        }
    }
    
    public static void log(HttpServletRequest req, int status) {
        String log = getLogMessage(req, status);
        if (status < 400) {
            auditLogger.info(log);
        } else {
            auditLogger.warn(log);
        }
    }
    
    //called by session invalidation (no request context)
    public static void logSessionInvalidation(HttpSession httpSession) {
        Session session = AbstractServices.getSession(httpSession);
        if (session != null && session.getUser() != null && session.getUser().getSessionUsername() != null) {
            AuditMessage msg = new AuditMessage();
            msg.req = "Session invalidation";
            msg.sesId = httpSession.getId();
            msg.user = session.getUser().getSessionUsername();
            auditLogger.info(msg.toLogString());
        }
    }

    public static void logPasswordEvent(String description, String user) {
        AuditMessage msg = new AuditMessage();
        msg.req = description;
        msg.user = user;
        auditLogger.info(msg.toLogString());
    }
    
    private static String getLogMessage(HttpServletRequest req, int status)  {
        String forwardedFor = req.getHeader("X-Forwarded-For");
        String source;
        try {
            source = Objects.requireNonNullElse(forwardedFor,  req.getRemoteAddr()+":"+req.getRemotePort());
        } catch (Exception e) {
            source = "unknown";
        }
        Optional<HttpSession> maybeHttpSession = Optional.ofNullable(req.getSession(false));
        String user;
        try {
            user = maybeHttpSession.map(AbstractServices::getSession).map(Session::getUser).map(AbstractUser::getSessionUsername).orElse(null);
        } catch (Exception e) {
            // shouldn't happen
            user = "unknown";
        }
        AuditMessage msg = new AuditMessage();
        msg.req = req.getMethod() + " " + req.getRequestURI();
        maybeHttpSession.ifPresent(s -> msg.sesId = s.getId()); // otherwise will use default "-"
        msg.src = source;
        if (user != null) { // otherwise will stay at default "-"
            msg.user = user;
        }
        msg.agent = req.getHeader("User-Agent");
        msg.sc = status;

        return msg.toLogString();
    }
    
    public static class AuditMessage {
        String req = "-";
        String sesId = "-";
        String src = "-";
        String user = "-";
        String agent = "-";
        int sc = -1;
        
        public AuditMessage(){super();}

        public String getReq() {
            return req;
        }

        public void setReq(String req) {
            this.req = req;
        }

        public String getSesId() {
            return sesId;
        }

        public void setSesId(String sesId) {
            this.sesId = sesId;
        }

        public String getSrc() {
            return src;
        }

        public void setSrc(String src) {
            this.src = src;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getAgent() {
            return agent;
        }

        public void setAgent(String agent) {
            this.agent = agent;
        }

        public int getSc() {
            return sc;
        }

        public void setSc(int sc) {
            this.sc = sc;
        }

        @Override
        public String toString() {
            return "AuditMessage{" +
                    "req='" + req + '\'' +
                    ", sesId='" + sesId + '\'' +
                    ", src='" + src + '\'' +
                    ", user='" + user + '\'' +
                    ", agent='" + agent + '\'' +
                    ", sc=" + sc +
                    '}';
        }

        public String toLogString() {
            try {
                return objectMapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                return "Message could not be serialized for " + this;
            }
        }
    }

    private static class EntityModificationLogMessage {
        // keep the field naming consistent with the other messages
        public String user;
        public String operation;
        public String type;
        public String name;
        public String id;
        public Map<String, String> attributes;

        @Override
        public String toString() {
            try {
                return objectMapper.writeValueAsString(this);
            } catch (JsonProcessingException e) {
                // format mimics the "default" toString()
                return "Unexpected error serializing " + getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(this));
            }
        }
    }


    public static void logEntityModification(Session<? extends AbstractUser> userSession, String operation, String entityTypeName, AbstractOrganizableObject entity, ObjectEnricher objectEnricher) {
        logEntityModification(userSession, operation, entityTypeName, entity, objectEnricher, null);
    }

    public static void logEntityModification(Session<? extends AbstractUser> userSession, String operation, String entityTypeName, AbstractOrganizableObject entity, ObjectEnricher objectEnricher, Map<String, String> moreAttributes) {
        if (entity != null) {
            LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
            Optional.ofNullable(objectEnricher).map(ObjectEnricher::getAdditionalAttributes).ifPresent(attributes::putAll);
            Optional.ofNullable(moreAttributes).ifPresent(attributes::putAll);

            modify(userSession, operation, entityTypeName,
                    Optional.ofNullable(entity.getId()).map(ObjectId::toString).orElse(null),
                    entity.getAttribute(AbstractOrganizableObject.NAME),
                    attributes
            );
        }
    }

    public static void logEntityModification(Session<? extends AbstractUser> userSession, String operation, String entityTypeName, String entityId, String entityName, Map<String, String> attributes) {
        modify(userSession, operation, entityTypeName, entityId, entityName, attributes);
    }

    // This method is named "modify" because that's what will appear in the logs if configured to use the separate audit log file:
    // {"timestamp":"2025-10-15 15:06:52,709","method":"modify","msg":{...}}
    private static void modify(Session<?> userSession, String operation, String entityTypeName, String entityId, String entityName, Map<String, String> attributes) {
        if (!isEntityModificationsLoggingEnabled()) {
            // We check (possibly again) if logging is enabled at all in case the caller didn't do it -- this adds no measurable overhead
            return;
        }
        EntityModificationLogMessage msg = new EntityModificationLogMessage();
        // all the following code is guaranteed to never produce NPE
        msg.user = Optional.ofNullable(userSession).map(Session::getUser).map(AbstractUser::getSessionUsername).orElse(null);
        msg.operation = operation;
        msg.type = entityTypeName;
        msg.name = entityName;
        msg.id = entityId;
        msg.attributes = (attributes != null && attributes.isEmpty()) ? null : attributes; // omit null or empty attributes
        auditLogger.info(msg.toString());
    }

}
