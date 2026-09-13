fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(
        &[
            "../../src/proto/container.proto", // Adjust path relative to your workspace proto definitions
        ],
        &["../../src/proto/"],
    )?;
    Ok(())
}
