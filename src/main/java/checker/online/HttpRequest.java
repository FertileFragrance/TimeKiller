package checker.online;

public class HttpRequest {
    private final String content;

    public HttpRequest(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }
}
