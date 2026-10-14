namespace Silex.Tool;

internal static class UiAssets
{
    internal sealed class PublicAsset;

    public static void Map(WebApplication app)
    {
        var assembly = typeof(UiAssets).Assembly;
        foreach (var name in assembly.GetManifestResourceNames().Where(name => name.StartsWith("ui/", StringComparison.Ordinal)))
        {
            using var input = assembly.GetManifestResourceStream(name) ?? throw new IOException($"Missing UI resource '{name}'.");
            using var output = new MemoryStream();
            input.CopyTo(output);
            var bytes = output.ToArray();
            var path = name["ui".Length..];
            var contentType = path.EndsWith(".js", StringComparison.Ordinal) ? "text/javascript" :
                path.EndsWith(".css", StringComparison.Ordinal) ? "text/css" : "text/html";
            RequestDelegate handler = async context =>
            {
                context.Response.ContentType = contentType;
                context.Response.ContentLength = bytes.Length;
                context.Response.Headers.CacheControl = "no-store";
                context.Response.Headers["X-Content-Type-Options"] = "nosniff";
                context.Response.Headers["Referrer-Policy"] = "no-referrer";
                context.Response.Headers["Content-Security-Policy"] =
                    "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'";
                await context.Response.Body.WriteAsync(bytes, context.RequestAborted);
            };
            app.MapGet("/ui" + path, handler).WithMetadata(new PublicAsset());
            if (path == "/index.html") app.MapGet("/ui/", handler).WithMetadata(new PublicAsset());
        }
    }
}
