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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.framework.server.AbstractServices;
import step.framework.server.Session;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import java.util.Objects;

public class AuditLogger {
    private static final Logger auditLogger = LoggerFactory.getLogger("AuditLogger");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void logResponse(HttpServletRequest req, int status) {
        if ((auditLogger.isTraceEnabled() || req.getRequestURI().equals("/rest/access/login")) &&
                !req.getRequestURI().equals("/rest/access/logout")) {
          log(req, status);  
        }
    }
    
    public static void log(HttpServletRequest req, int status) {
        String log = getLogMessage(req, status);
        if (status < 400){
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
        String user;
        try {
            user = AbstractServices.getSession(req.getSession()).getUser().getSessionUsername();
        } catch (Exception e) {
            user = "unknown";
        }
        AuditMessage msg = new AuditMessage();
        msg.req = req.getMethod() + " " + req.getRequestURI(); 
        msg.sesId = req.getSession().getId();
        msg.src = source;
        msg.user = user;
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




}
