import json
import os
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

CONNECTIONS_FILE = "connections.json"

def _get_connections_path() -> Path:
    """Get the path to the connections file"""
    return Path(__file__).parent / CONNECTIONS_FILE

def _ensure_file_exists():
    """Ensure the connections file exists"""
    path = _get_connections_path()
    if not path.exists():
        with open(path, 'w') as f:
            json.dump({"connections": []}, f, indent=2)

def load_connections() -> List[Dict[str, Any]]:
    """Load all connections from the JSON file"""
    _ensure_file_exists()
    try:
        with open(_get_connections_path(), 'r') as f:
            data = json.load(f)
            return data.get("connections", [])
    except (json.JSONDecodeError, FileNotFoundError):
        return []

def save_connections(connections: List[Dict[str, Any]]) -> bool:
    """Save connections to the JSON file"""
    try:
        with open(_get_connections_path(), 'w') as f:
            json.dump({"connections": connections}, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving connections: {e}")
        return False

def get_connection(connection_id: str) -> Optional[Dict[str, Any]]:
    """Get a specific connection by ID"""
    connections = load_connections()
    for conn in connections:
        if conn.get("id") == connection_id:
            return conn
    return None

def add_connection(name: str, source: Dict[str, Any], target: Dict[str, Any]) -> Dict[str, Any]:
    """Add a new connection"""
    connections = load_connections()
    
    new_connection = {
        "id": str(uuid.uuid4()),
        "name": name,
        "source": source,
        "target": target,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    connections.append(new_connection)
    save_connections(connections)
    
    return new_connection

def invert_connection(connection_id: str, name_suffix: str = " (inverted)") -> Optional[Dict[str, Any]]:
    """Create a new connection by swapping source and target from an existing one."""
    original = get_connection(connection_id)
    if not original:
        return None

    source = dict(original.get("source", {}))
    target = dict(original.get("target", {}))
    inverted_name = f"{original.get('name', 'Connection')}{name_suffix}"

    return add_connection(inverted_name, target, source)

def update_connection(connection_id: str, name: str, source: Dict[str, Any], target: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Update an existing connection"""
    connections = load_connections()
    
    for i, conn in enumerate(connections):
        if conn.get("id") == connection_id:
            connections[i] = {
                "id": connection_id,
                "name": name,
                "source": source,
                "target": target,
                "created_at": conn.get("created_at", datetime.now().isoformat()),
                "updated_at": datetime.now().isoformat()
            }
            save_connections(connections)
            return connections[i]
    
    return None

def delete_connection(connection_id: str) -> bool:
    """Delete a connection by ID"""
    connections = load_connections()
    original_length = len(connections)
    
    connections = [conn for conn in connections if conn.get("id") != connection_id]
    
    if len(connections) < original_length:
        save_connections(connections)
        return True
    
    return False

def get_connections_without_passwords() -> List[Dict[str, Any]]:
    """Get all connections but exclude passwords for security"""
    connections = load_connections()
    safe_connections = []
    
    for conn in connections:
        safe_conn = {
            "id": conn.get("id"),
            "name": conn.get("name"),
            "source": {
                "host": conn.get("source", {}).get("host"),
                "port": conn.get("source", {}).get("port"),
                "database": conn.get("source", {}).get("database"),
                "username": conn.get("source", {}).get("username")
            },
            "target": {
                "host": conn.get("target", {}).get("host"),
                "port": conn.get("target", {}).get("port"),
                "database": conn.get("target", {}).get("database"),
                "username": conn.get("target", {}).get("username")
            },
            "created_at": conn.get("created_at"),
            "updated_at": conn.get("updated_at")
        }
        safe_connections.append(safe_conn)
    
    return safe_connections
