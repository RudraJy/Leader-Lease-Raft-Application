# Raft Consensus Algorithm Implementation

## Overview

This project is an implementation of the Raft consensus algorithm in Python. Raft is a consensus algorithm designed as an alternative to Paxos, aiming to be more understandable and easier to implement.

## Features

- **Leader Election**: Handles the election of a leader among nodes.
- **Log Replication**: Ensures that the logs are consistently replicated across the nodes.
- **Fault Tolerance**: Continues to operate correctly even if some nodes fail.
- **Multithreading**: Uses multithreading for efficient processing.
- **gRPC Communication**: Employs gRPC for robust communication between server-server and client-server nodes.

## Tech Stack

- **Python**: Core language for implementation.
- **gRPC**: For communication between nodes.
- **Multithreading**: For concurrent operations.

## Installation

1. **Clone the repository:**
    ```bash
    git clone https://github.com/yourusername/raft-consensus-algorithm.git
    cd raft-consensus-algorithm
    ```

2. **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1. **Start the server nodes:**
    ```bash
    python server.py --id <node_id>
    ```

2. **Start the client:**
    ```bash
    python client.py
    ```

3. **Interacting with the system:**
    - Use the client to send commands to the Raft cluster.
    - Monitor logs to observe the consensus process.

## Code Structure

- **server.py**: Contains the implementation of the server nodes.
- **client.py**: Contains the client-side code to interact with the Raft cluster.
- **raft.py**: Core implementation of the Raft algorithm.
- **logs/**: Directory where logs are stored for each node.

## Examples

### Starting a Server Node
```bash
python server.py --id 1
