package com.surya.DistributedDataPlatform.common.logging;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/** Adds a request correlation ID and records one concise access log per HTTP request. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 20)
public class RequestLoggingFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);
    private static final String REQUEST_ID_HEADER = "X-Request-ID";

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path.startsWith("/actuator/health") || path.startsWith("/actuator/info");
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {

        String requestId = UUID.randomUUID().toString();
        long startedAt = System.nanoTime();
        MDC.put("requestId", requestId);
        MDC.put("httpMethod", request.getMethod());
        MDC.put("httpPath", request.getRequestURI());
        response.setHeader(REQUEST_ID_HEADER, requestId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            long durationMs = (System.nanoTime() - startedAt) / 1_000_000;
            int status = response.getStatus();
            MDC.put("httpStatus", Integer.toString(status));
            MDC.put("durationMs", Long.toString(durationMs));

            if (status >= 500) {
                log.error("HTTP request completed with server error");
            } else if (status >= 400) {
                log.warn("HTTP request completed with client error");
            } else {
                log.info("HTTP request completed");
            }
            MDC.clear();
        }
    }
}
