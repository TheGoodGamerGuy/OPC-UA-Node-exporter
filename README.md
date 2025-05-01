# OPC UA Node Exporter

## Description

This Python script connects to an OPC UA server, browses its node structure starting from the 'Objects' node, and exports information about the nodes to a CSV file. It uses asynchronous operations and batch processing for efficiency and supports optional filtering by namespace index and data type.
Performance prioritized.

## Requirements

* Python 3.x
* Required Python libraries:
    * `asyncua`
    * `asyncio`
    * `logging`
    * `csv`


## Configuration

Before running the script, you **must** modify the hardcoded configuration values within the `main` function in the `exporter.py` file.

1.  **`HARDCODED_SERVER_URL`**: Set this to the full URL of your target OPC UA server (e.g., `"opc.tcp://your.server.address:4840"`).
2.  **`HARDCODED_OUTPUT_FILE`**: Specify the desired path and filename for the output CSV file (e.g., `"exported_nodes.csv"`).
3.  **`HARDCODED_NAMESPACE_FILTER`**:
    * To export nodes only from a specific namespace, set this to the integer index of that namespace (e.g., `2`).
    * To export nodes from all namespaces, set this to `None`.
4.  **`HARDCODED_DATATYPE_FILTER`**:
    * To export only nodes of a specific data type, set this to the string name of the data type (e.g., `"Float"`, `"Int32"`). The comparison is case-insensitive.
    * To export nodes regardless of their data type, set this to `None`.
5.  **`HARDCODED_BATCH_SIZE`**: (Optional) Adjust the number of nodes processed in each batch. The default is `1000`. Larger values might improve speed but increase memory usage.
6.  **`HARDCODED_LOGLEVEL`**: (Optional) Change the logging verbosity. Options include `'DEBUG'`, `'INFO'`, `'WARNING'`, `'ERROR'`, `'CRITICAL'`. The default is `'INFO'`.

## Usage

1.  Ensure Python and the required `asyncua` library are installed.
2.  Modify the hardcoded configuration values in `exporter.py` as described above.
3.  Run the script from your terminal:

    ```bash
    python exporter.py
    ```
4.  The script will connect to the server, browse the nodes, apply filters, read node attributes, and write the results to the specified output CSV file. Progress and summary information will be printed to the console.

## Output Format

The output CSV file (`HARDCODED_OUTPUT_FILE`) will contain the following columns:

* `NodeId`: The full NodeId string (e.g., "ns=2;i=1234").
* `BrowseName`: The BrowseName of the node.
* `CustomName`: Prefilled with the BrowseName of the node, is used for the telegraf configuration generator.
* `DataType`: The resolved BrowseName of the node's data type (e.g., "Float", "String", "Int32") or "N/A" if it couldn't be read/resolved.
* `DisplayName`: The DisplayName of the node.
* `Description`: The Description of the node (if available).

If reading an attribute fails for a node, "N/A" might be used as a placeholder in the respective cell. Nodes filtered out by the DataType filter will not appear in the CSV.
