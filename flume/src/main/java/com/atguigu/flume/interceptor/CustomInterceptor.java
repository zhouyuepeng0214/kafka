package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class CustomInterceptor implements Interceptor {

    public void initialize() {

    }

    public Event intercept(Event event) {

        if (event.getBody()[0] >= '0' && event.getBody()[0] <= '9') {
            event.getHeaders().put("topic","number");
        } else if (event.getBody()[0] >= 'a' && event.getBody()[0] <= 'z') {
            event.getHeaders().put("topic","letter");
        }
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new CustomInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
