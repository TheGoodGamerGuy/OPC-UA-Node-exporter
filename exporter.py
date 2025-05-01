import asyncio
import logging
import time
import csv
from asyncio import Queue
from typing import List, Optional, Dict, Any, Set, Tuple, Union
from asyncua import Client, ua, Node
from asyncua.ua.uaerrors import UaStatusCodeError
# argparse is removed as values are hardcoded

# Configure logging (initial setup, level might be overridden in main)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logging.getLogger('asyncua').setLevel(logging.WARNING) # Keep asyncua less verbose

class NodeCSVExporter:
    """
    Exports OPC UA nodes using optimized batch operations, streaming CSV,
    optional DataType filtering, and a two-stage read process for potentially
    better performance with highly selective data type filters.
    """
    # __init__ remains the same as it receives values from the main function
    def __init__(self, server_url: str, output_file: str, namespace_filter: Optional[int] = None, datatype_filter: Optional[str] = None, batch_size: int = 1000):
        if not server_url:
            raise ValueError("Server URL cannot be empty.")
        if not output_file:
            raise ValueError("Output file path cannot be empty.")

        self.server_url: str = server_url
        self.output_file: str = output_file
        self.namespace_filter: Optional[int] = namespace_filter
        # Ensure datatype filter is lowercase if provided
        self.datatype_filter: Optional[str] = datatype_filter.lower() if datatype_filter else None
        self.batch_size: int = batch_size
        self.processed_node_ids: Set[ua.NodeId] = set()
        self.nodes_to_export_ids: List[ua.NodeId] = []
        self.client: Optional[Client] = None
        self._browse_queue: Queue[Node] = Queue()
        self._start_time: float = 0.0
        self._total_processed_browse: int = 0
        self._total_exported_nodes: int = 0
        self._nodes_with_read_errors: int = 0 # Counts nodes with errors in *either* read stage
        self.datatype_cache: Dict[ua.NodeId, str] = {}
        self._nodes_filtered_by_datatype: int = 0


    async def _browse_nodes_recursive(self, start_node: Node):
        """ Browsing logic remains the same """
        await self._browse_queue.put(start_node)
        self.processed_node_ids.add(start_node.nodeid)
        self._total_processed_browse = 0
        browse_start_time = time.time()
        last_log_time = time.time()
        logger.info("Starting node browse...")
        while not self._browse_queue.empty():
            batch_nodes: List[Node] = []
            # Dequeue up to batch_size nodes
            for _ in range(min(self.batch_size, self._browse_queue.qsize())):
                node = await self._browse_queue.get()
                batch_nodes.append(node)

            # Get children for the batch concurrently
            children_tasks = [node.get_children(refs=ua.ObjectIds.HierarchicalReferences) for node in batch_nodes]
            try:
                children_results: List[Union[List[Node], Exception]] = await asyncio.gather(*children_tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"Critical error during batch get_children gather: {e}", exc_info=True)
                continue # Skip this batch if gather fails catastrophically

            # Process results for each parent node in the batch
            for i, result in enumerate(children_results):
                parent_node = batch_nodes[i]
                if isinstance(result, Exception):
                    logger.warning(f"Could not get children for node {parent_node.nodeid}: {result}")
                    continue # Skip children for this specific node

                # Add new children to the queue
                for child in result:
                    if child.nodeid not in self.processed_node_ids:
                        self.processed_node_ids.add(child.nodeid)
                        await self._browse_queue.put(child)

            # Update progress counter
            self._total_processed_browse += len(batch_nodes)

            # Log progress periodically or at the end
            current_time = time.time()
            if current_time - last_log_time >= 1.0 or self._browse_queue.empty():
                elapsed_time = current_time - browse_start_time
                nodes_per_second = self._total_processed_browse / elapsed_time if elapsed_time > 0 else 0
                logger.info(
                    f"Browsing... Processed: {self._total_processed_browse}, "
                    f"Queue: {self._browse_queue.qsize()}, "
                    f"Unique Nodes Found: {len(self.processed_node_ids)}, "
                    f"Speed: {nodes_per_second:.2f} nodes/s"
                )
                last_log_time = current_time
        logger.info(f"Finished browsing. Total unique nodes found: {len(self.processed_node_ids)}")

    async def _batch_read_datatypes(self, node_ids: List[ua.NodeId]) -> Dict[ua.NodeId, Optional[ua.NodeId]]:
        """
        Reads ONLY the DataType attribute for a batch of NodeIds.

        Returns:
            A dictionary mapping NodeId to its DataType NodeId (or None if read failed).
        """
        if not self.client or not node_ids: return {nid: None for nid in node_ids}

        read_params = ua.ReadParameters()
        for node_id in node_ids:
            # Create ReadValueId to specify NodeId and AttributeId
            rv = ua.ReadValueId()
            rv.NodeId = node_id
            rv.AttributeId = ua.AttributeIds.DataType # Target the DataType attribute
            read_params.NodesToRead.append(rv)

        logger.debug(f"Performing batch read stage 1 (DataType) for {len(node_ids)} nodes")
        try:
            # Perform the asynchronous read operation
            results = await self.client.uaclient.read(read_params)
        except Exception as e:
            logger.error(f"Batch read stage 1 (DataType) failed: {e}", exc_info=True)
            return {nid: None for nid in node_ids} # Return None for all on batch failure

        datatype_nodeids = {}
        # Process the results for each node
        for i, node_id in enumerate(node_ids):
            data_value = results[i]
            if data_value.StatusCode.is_good() and data_value.Value and data_value.Value.Value:
                # Store the NodeId of the DataType
                datatype_nodeids[node_id] = data_value.Value.Value
            else:
                datatype_nodeids[node_id] = None # Mark as None if read failed
                logger.debug(f"Failed to read DataType for Node {node_id.to_string()}: {data_value.StatusCode.name}")
        return datatype_nodeids

    async def _batch_read_other_attributes(self, node_ids: List[ua.NodeId]) -> Dict[ua.NodeId, Dict[str, Any]]:
        """
        Reads BrowseName, DisplayName, Description for a batch of NodeIds.

        Returns:
            A dictionary mapping NodeId to a dict containing {'BrowseName': ..., 'DisplayName': ..., 'Description': ..., '_read_error': bool}.
        """
        if not self.client or not node_ids: return {nid: {"_read_error": True} for nid in node_ids}

        read_params = ua.ReadParameters()
        # Define the attributes we want to read in this stage
        attributes_to_read = [
            ua.AttributeIds.BrowseName,
            ua.AttributeIds.DisplayName,
            ua.AttributeIds.Description
        ]
        num_attrs = len(attributes_to_read)

        # Create ReadValueId for each node and each attribute
        for node_id in node_ids:
            for attr_id in attributes_to_read:
                rv = ua.ReadValueId()
                rv.NodeId = node_id
                rv.AttributeId = attr_id
                read_params.NodesToRead.append(rv)

        logger.debug(f"Performing batch read stage 2 (Other Attrs) for {len(node_ids)} nodes")
        try:
            # Perform the asynchronous read operation
            results = await self.client.uaclient.read(read_params)
        except Exception as e:
            logger.error(f"Batch read stage 2 (Other Attrs) failed: {e}", exc_info=True)
            return {nid: {"_read_error": True} for nid in node_ids} # Mark all as errored

        processed_data = {}
        # Process the results, grouping by NodeId
        for i, node_id in enumerate(node_ids):
            node_data = {"_read_error": False}
            has_error_in_node = False
            # Iterate through the results for the attributes of the current node
            for j, attr_id in enumerate(attributes_to_read):
                result_index = i * num_attrs + j # Calculate index in the flat results list
                data_value = results[result_index]
                attr_value = None

                if data_value.StatusCode.is_good():
                    if data_value.Value and data_value.Value.Value is not None:
                        attr_value = data_value.Value.Value
                else:
                    # Log if reading a specific attribute failed
                    logger.debug(f"Failed to read Attr {attr_id} for Node {node_id.to_string()} (Stage 2): {data_value.StatusCode.name}")
                    has_error_in_node = True

                # Extract the value based on the attribute type
                if attr_id == ua.AttributeIds.BrowseName:
                    # BrowseName is a QualifiedName object
                    node_data["BrowseName"] = attr_value.Name if attr_value else "N/A"
                elif attr_id == ua.AttributeIds.DisplayName:
                    # DisplayName is a LocalizedText object
                    node_data["DisplayName"] = attr_value.Text if attr_value else "N/A"
                elif attr_id == ua.AttributeIds.Description:
                     # Description is a LocalizedText object
                    node_data["Description"] = attr_value.Text if attr_value else "N/A"

            # Mark the entire node's data as having an error if any attribute read failed
            if has_error_in_node:
                 node_data["_read_error"] = True
            processed_data[node_id] = node_data

        return processed_data


    async def _resolve_datatype_names(self, datatype_nodeids: Set[ua.NodeId]):
        """
        Resolves the BrowseName for a set of DataType NodeIds using batch reads and a cache.
        """
        if not self.client or not datatype_nodeids: return

        # Identify NodeIds whose names are not already in the cache
        nodeids_to_resolve = {nid for nid in datatype_nodeids if nid not in self.datatype_cache and nid != ua.NodeId(ua.ObjectIds.Null)}
        if not nodeids_to_resolve:
            logger.debug("All required DataType names already cached.")
            return

        logger.info(f"Resolving BrowseNames for {len(nodeids_to_resolve)} new DataType NodeIds...")
        read_params = ua.ReadParameters()
        nodeid_list = list(nodeids_to_resolve) # Convert set to list for indexing
        # Prepare batch read for BrowseName attribute
        for node_id in nodeid_list:
            rv = ua.ReadValueId()
            rv.NodeId = node_id
            rv.AttributeId = ua.AttributeIds.BrowseName # Target the BrowseName
            read_params.NodesToRead.append(rv)

        try:
            # Perform the batch read
            results = await self.client.uaclient.read(read_params)
        except Exception as e:
            logger.error(f"Batch read for DataType names failed: {e}", exc_info=True)
            # Fallback: use NodeId string as name if read fails
            for nid in nodeid_list: self.datatype_cache[nid] = nid.to_string()
            return

        resolved_count, failed_count = 0, 0
        # Process results and update the cache
        for i, node_id in enumerate(nodeid_list):
            data_value = results[i]
            if data_value.StatusCode.is_good() and data_value.Value and data_value.Value.Value:
                # Store the resolved name (e.g., "Float", "Int32")
                self.datatype_cache[node_id] = data_value.Value.Value.Name
                resolved_count += 1
            else:
                # Fallback: use NodeId string if resolution failed
                self.datatype_cache[node_id] = node_id.to_string()
                failed_count += 1
                logger.warning(f"Failed to resolve BrowseName for DataType NodeId {node_id}: {data_value.StatusCode}")
        logger.info(f"DataType resolution update complete. Newly Resolved: {resolved_count}, Failed/Fallback: {failed_count}")


    async def export_to_csv(self):
        """
        Filters nodes by namespace and optionally DataType, then exports details
        to a CSV file using a two-stage read process.
        """
        if not self.processed_node_ids:
            logger.warning("No nodes were found during browsing. Skipping export.")
            return

        # --- 1. Namespace Filtering ---
        logger.info("Filtering nodes by namespace...")
        nodes_after_ns_filter: List[ua.NodeId] = []
        filtered_out_ns = 0
        if self.namespace_filter is not None:
            # Keep only nodes matching the specified namespace index
            nodes_after_ns_filter = [nid for nid in self.processed_node_ids if nid.NamespaceIndex == self.namespace_filter]
            filtered_out_ns = len(self.processed_node_ids) - len(nodes_after_ns_filter)
            logger.info(f"Nodes after namespace filtering (Namespace={self.namespace_filter}): {len(nodes_after_ns_filter)}")
            logger.info(f"Nodes filtered out by namespace: {filtered_out_ns}")
        else:
            # No namespace filter applied, use all browsed nodes
            nodes_after_ns_filter = list(self.processed_node_ids)
            logger.info("No namespace filter applied.")

        self.nodes_to_export_ids = nodes_after_ns_filter
        if not self.nodes_to_export_ids:
            logger.warning("No nodes remaining after namespace filter. Skipping export.")
            return

        # --- 2. Prepare for Export ---
        self._total_exported_nodes = 0 # Tracks nodes processed through stage 1
        self._nodes_with_read_errors = 0 # Tracks nodes with errors in stage 1 OR stage 2
        self._nodes_filtered_by_datatype = 0 # Tracks nodes filtered out by DataType
        rows_written = 0 # Tracks rows actually written to CSV

        export_start_message = f"Starting export of {len(self.nodes_to_export_ids)} nodes"
        if self.datatype_filter: export_start_message += f" (filtering for DataType: '{self.datatype_filter}')"
        export_start_message += " using two-stage read..."
        logger.info(export_start_message)

        export_start_time = time.time()
        last_log_time = time.time()
        # Define the columns for the CSV file
        expected_columns = ["NodeId", "BrowseName", "DataType", "DisplayName", "Description"]

        try:
            # Open the CSV file for writing
            with open(self.output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=expected_columns, extrasaction='ignore')
                writer.writeheader() # Write the header row

                # --- 3. Process and Write in Batches (Two-Stage Read) ---
                for i in range(0, len(self.nodes_to_export_ids), self.batch_size):
                    batch_ids = self.nodes_to_export_ids[i : i + self.batch_size]

                    # === Stage 1: Read DataTypes for the current batch ===
                    batch_datatype_nodeids = await self._batch_read_datatypes(batch_ids)

                    # === Resolve DataType names needed for this batch ===
                    unique_datatypes_in_batch = {dt_nid for dt_nid in batch_datatype_nodeids.values() if dt_nid}
                    await self._resolve_datatype_names(unique_datatypes_in_batch)

                    # === Filter nodes based on resolved DataType & prepare for Stage 2 ===
                    filtered_batch_ids_for_stage2: List[ua.NodeId] = []
                    data_for_filtered_nodes: Dict[ua.NodeId, Dict[str, Any]] = {} # Store partial data

                    for node_id in batch_ids:
                        dt_nodeid = batch_datatype_nodeids.get(node_id) # Get DataType NodeId from Stage 1 result
                        resolved_dt_name = "N/A"
                        read_error_stage1 = False

                        if dt_nodeid is None: # Read DataType failed in Stage 1
                             read_error_stage1 = True
                        elif dt_nodeid in self.datatype_cache:
                            resolved_dt_name = self.datatype_cache[dt_nodeid] # Use cached name
                        elif dt_nodeid == ua.NodeId(ua.ObjectIds.Null):
                             resolved_dt_name = "Null" # Handle Null DataType explicitly
                        else: # Fallback if resolution failed (should be rare)
                             resolved_dt_name = dt_nodeid.to_string()

                        # Apply DataType filter if specified
                        include_row = True
                        if self.datatype_filter:
                            # Compare lowercase resolved name with lowercase filter
                            if not resolved_dt_name or resolved_dt_name.lower() != self.datatype_filter:
                                include_row = False
                                self._nodes_filtered_by_datatype += 1

                        # If the node passes the filter (or no filter is active)
                        if include_row:
                            filtered_batch_ids_for_stage2.append(node_id)
                            # Store NodeId and resolved DataType name, mark if Stage 1 had error
                            data_for_filtered_nodes[node_id] = {
                                "NodeId": node_id.to_string(),
                                "DataType": resolved_dt_name,
                                "_read_error": read_error_stage1 # Track error from stage 1
                            }
                        # If node is filtered out BUT had a read error in stage 1, count it
                        elif read_error_stage1:
                            self._nodes_with_read_errors += 1


                    # === Stage 2: Read Other Attributes ONLY for Filtered Nodes ===
                    final_batch_data_to_write = [] # List to hold final dicts for CSV writing
                    if filtered_batch_ids_for_stage2:
                        # Perform batch read for BrowseName, DisplayName, Description
                        other_attrs_results = await self._batch_read_other_attributes(filtered_batch_ids_for_stage2)

                        # Combine Stage 1 and Stage 2 results
                        for node_id in filtered_batch_ids_for_stage2:
                            final_node_data = data_for_filtered_nodes[node_id] # Get partial data from Stage 1
                            other_attrs = other_attrs_results.get(node_id, {"_read_error": True}) # Get Stage 2 results

                            # Add attributes from Stage 2, using "N/A" as fallback
                            final_node_data["BrowseName"] = other_attrs.get("BrowseName", "N/A")
                            final_node_data["DisplayName"] = other_attrs.get("DisplayName", "N/A")
                            final_node_data["Description"] = other_attrs.get("Description", "N/A")

                            # Check if Stage 2 introduced an error
                            stage2_error = other_attrs.get("_read_error", False)

                            # If Stage 2 failed AND Stage 1 succeeded for this node, increment error count
                            if stage2_error and not final_node_data["_read_error"]:
                                self._nodes_with_read_errors += 1
                                final_node_data["_read_error"] = True # Mark the node as having an error overall

                            # Remove the internal error tracking flag before writing
                            final_node_data.pop("_read_error", None)
                            final_batch_data_to_write.append(final_node_data)

                    # === Stage 3: Write the processed batch to CSV ===
                    if final_batch_data_to_write:
                        writer.writerows(final_batch_data_to_write)
                        rows_written += len(final_batch_data_to_write)

                    # === Update and Log Progress ===
                    self._total_exported_nodes += len(batch_ids) # Count nodes processed (passed to stage 1)
                    current_time = time.time()
                    # Log progress every second or at the very end
                    if current_time - last_log_time >= 1.0 or self._total_exported_nodes >= len(self.nodes_to_export_ids):
                        elapsed_time = current_time - export_start_time
                        nodes_per_second = self._total_exported_nodes / elapsed_time if elapsed_time > 0 else 0
                        logger.info(
                             f"Exporting (2-Stage)... Processed: {self._total_exported_nodes}/{len(self.nodes_to_export_ids)}, "
                             f"Errors: {self._nodes_with_read_errors}, "
                             f"Filtered DT: {self._nodes_filtered_by_datatype}, "
                             f"Written: {rows_written}, "
                             f"Speed: {nodes_per_second:.2f} nodes/s"
                        )
                        last_log_time = current_time

            logger.info(f"Finished writing {rows_written} rows to CSV: {self.output_file}")

        except IOError as e:
            logger.error(f"Failed to write to CSV file {self.output_file}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred during CSV export: {e}", exc_info=True)

        # --- 4. Final Summary ---
        logger.info("--- Export Summary ---")
        logger.info(f"Total unique nodes found during browse: {len(self.processed_node_ids)}")
        if self.namespace_filter is not None:
             logger.info(f"Nodes matching namespace {self.namespace_filter}: {len(self.nodes_to_export_ids)}")
             logger.info(f"Nodes filtered out by namespace: {filtered_out_ns}")
        else:
             logger.info(f"Nodes initially targeted for export: {len(self.nodes_to_export_ids)}") # Same as processed_node_ids if no NS filter
        if self.datatype_filter:
             logger.info(f"Nodes filtered out by DataType ('{self.datatype_filter}'): {self._nodes_filtered_by_datatype}")
        logger.info(f"Rows successfully written to CSV: {rows_written}")
        logger.info(f"Nodes with attribute read errors (among processed): {self._nodes_with_read_errors}")
        logger.info(f"CSV file location: {self.output_file}")
        logger.info("----------------------")


    async def run(self):
        """ Orchestrates the connection, browsing, and exporting process. """
        self._start_time = time.time()
        try:
            # Connect to the OPC UA server
            async with Client(url=self.server_url, timeout=30) as client:
                self.client = client
                logger.info(f"Connected to OPC UA server: {self.server_url}")

                # Start browsing from the 'Objects' node
                logger.info("Starting node browse (from Objects node)...")
                objects_node = self.client.get_objects_node()
                await self._browse_nodes_recursive(objects_node)

                # Export the browsed and filtered nodes to CSV
                await self.export_to_csv()

        except (ua.UaError, ConnectionRefusedError, TimeoutError, OSError) as e:
             logger.error(f"Failed to connect or communicate with server {self.server_url}: {e}")
        except ValueError as e: # Catch init errors if they somehow occur later
             logger.error(f"Configuration error: {e}")
        except Exception as e:
             logger.error(f"An unexpected error occurred during the run: {e}", exc_info=True)
        finally:
            # Clean up resources
            self.client = None # Release client reference
            self.datatype_cache.clear() # Clear cache for potential next run
            total_duration = time.time() - self._start_time
            logger.info(f"Process finished. Total time: {total_duration:.2f} seconds.")


async def main():
    """
    Main entry point of the script. Configures logging and runs the exporter
    with hardcoded values.
    """
    # --- HARDCODED VALUES ---
    # !! IMPORTANT: Modify these values before running !!
    HARDCODED_SERVER_URL = "opc.tcp://100.94.111.58:4841" # Example: Replace with your server URL
    HARDCODED_OUTPUT_FILE = "nodes_output_new.csv"     # Example: Replace with your desired output path
    HARDCODED_NAMESPACE_FILTER = 2                 # Example: Set to an integer (e.g., 2) or None to disable
    HARDCODED_DATATYPE_FILTER = None                # Example: Set to a string (e.g., "Float", "Int32") or None to disable (case-insensitive)
    HARDCODED_BATCH_SIZE = 1000                       # Default batch size
    HARDCODED_LOGLEVEL = 'INFO'                       # Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    # --- END OF HARDCODED VALUES ---

    # Configure logging based on the hardcoded level
    app_log_level = getattr(logging, HARDCODED_LOGLEVEL.upper(), logging.INFO)
    # Remove existing handlers to avoid duplicate logs if run multiple times in same session
    for handler in logging.root.handlers[:]: logging.root.removeHandler(handler)
    logging.basicConfig(level=app_log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.getLogger('asyncua').setLevel(logging.WARNING) # Keep asyncua logs less verbose

    # Log the settings being used
    logger.info(f"Application log level set to: {HARDCODED_LOGLEVEL.upper()}")
    logger.info(f"Asyncua library log level set to: WARNING")
    logger.info("Starting OPC UA Exporter with hardcoded settings...")
    logger.info(f"Server URL: {HARDCODED_SERVER_URL}")
    logger.info(f"Output File: {HARDCODED_OUTPUT_FILE}")
    logger.info(f"Namespace Filter: {'All' if HARDCODED_NAMESPACE_FILTER is None else HARDCODED_NAMESPACE_FILTER}")
    logger.info(f"DataType Filter: {'All' if HARDCODED_DATATYPE_FILTER is None else HARDCODED_DATATYPE_FILTER}")
    logger.info(f"Batch Size: {HARDCODED_BATCH_SIZE}")

    try:
        # Create the exporter instance with the hardcoded values
        exporter = NodeCSVExporter(
            server_url=HARDCODED_SERVER_URL,
            output_file=HARDCODED_OUTPUT_FILE,
            namespace_filter=HARDCODED_NAMESPACE_FILTER,
            datatype_filter=HARDCODED_DATATYPE_FILTER,
            batch_size=HARDCODED_BATCH_SIZE
        )
        # Run the exporter's main logic
        await exporter.run()
    except ValueError as e:
        logger.error(f"Initialization Error: {e}") # Catch errors from NodeCSVExporter.__init__
    except Exception as e:
        logger.error(f"An unexpected error occurred in main setup or run: {e}", exc_info=True)

if __name__ == "__main__":
    # Run the main asynchronous function
    asyncio.run(main())

