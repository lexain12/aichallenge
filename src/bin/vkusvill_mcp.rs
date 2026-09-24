use rmcp::{ServiceExt, model::ClientConfig, transport::StreamableHttpClientTransport};

const VKUSVILL_MCP_URL: &str = "https://mcp.vkusvill.ru/mcp";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = StreamableHttpClientTransport::from_uri(VKUSVILL_MCP_URL);
    let client = ClientConfig::default().serve(transport).await?;
    let tools = client.list_tools(None).await?;

    println!("{}", serde_json::to_string_pretty(&tools)?);

    Ok(())
}
