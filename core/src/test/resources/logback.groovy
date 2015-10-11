appender("STDOUT", ConsoleAppender) {
    withJansi = true
    encoder(PatternLayoutEncoder) {
        pattern = "%green(«%F:%L») [%red(%date{HH:mm:ss.SSS})] %highlight(%-5level) %cyan(%logger{15}) - %msg %n"
    }
}


root(TRACE, ["STDOUT"])
