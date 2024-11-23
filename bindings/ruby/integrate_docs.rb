require "json"
require "open3"

RUST_DOC_JSON = "target/doc/opendal_ruby.json"
OUTPUT_DOC_FILE = "lib/generated_doc.rb" # Where the documentation will be written

# Load the Rust JSON documentation
def load_rust_docs
  JSON.parse(File.read(RUST_DOC_JSON))
end

# Extract all documented Rust functions
def extract_functions(rust_docs)
  rust_docs["index"].values.select do |item|
    item["inner"]["function"] && item["docs"]
  end
end

# Format Rust documentation for RDoc
def format_rdoc(rust_doc)
  rust_doc.split("\n").map { |line| "  # #{line.strip}" }.join("\n")
end

# Generate Ruby method stubs with documentation
def generate_ruby_methods(functions)
  methods = []
  functions.each do |function|
    name = function["name"]
    docs = format_rdoc(function["docs"])
    methods << <<~RUBY
      #{docs}
        def #{name}(*args)
          # Rust implementation placeholder
        end
    RUBY
  end
  methods
end

# Write the Ruby method stubs to a file
def write_to_file(output_file, methods)
  File.open(output_file, "w") do |file|
    file.puts "# Auto-generated Ruby methods with RDoc"
    file.puts "module RustBindings"
    file.puts methods.join("\n")
    file.puts "end"
  end
  puts "Generated RDoc-compatible Ruby methods in #{output_file}"
end

# Main script execution
puts "Loading Rust documentation..."
rust_docs = load_rust_docs
puts "Extracting documented functions..."
functions = extract_functions(rust_docs)
puts "Generating Ruby method stubs..."
methods = generate_ruby_methods(functions)
puts "Writing to Ruby file..."
write_to_file(OUTPUT_DOC_FILE, methods)
puts "Done!"
