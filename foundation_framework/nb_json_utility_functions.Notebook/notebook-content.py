# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ###### JSON File Handler

# CELL ********************


import re
import logging
from typing import List, Dict, Any, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType,
    FloatType, BooleanType, TimestampType, DateType, ArrayType, MapType,
    DecimalType
)


class JsonFileHandler:
    """
    Utilities for JSON DataFrame processing.

    Supports optional struct flattening, schema validation, type conversion,
    and preserving nested columns as JSON strings.
    """
    
    # Mapping from string type names to PySpark types
    TYPE_MAPPING = {
        'string': StringType(),
        'int': IntegerType(),
        'integer': IntegerType(),
        'long': LongType(),
        'bigint': LongType(),
        'double': DoubleType(),
        'float': FloatType(),
        'boolean': BooleanType(),
        'bool': BooleanType(),
        'timestamp': TimestampType(),
        'date': DateType(),
        'decimal': DecimalType(38, 10),
    }
    
    def __init__(self, expected_schema: Optional[List[Dict[str, Any]]] = None, separator: str = '_'):
        """
        Initialize the handler.

        Args:
            expected_schema: Expected output schema definitions.
            separator: Separator used in flattened column names.
        """
        self.expected_schema = expected_schema or []
        self.separator = separator
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Build lookup dict for quick access
        self._schema_lookup = {col['name']: col for col in self.expected_schema}
        self._schema_lookup_ci = {col['name'].lower(): col for col in self.expected_schema}
    
    def _get_spark_type(self, type_name: str):
        """
        Map a type name string to a PySpark DataType.
        """
        type_lower = type_name.lower().strip()
        
        # Handle parameterized types like decimal(18,2)
        if type_lower.startswith('decimal'):
            match = re.match(r'decimal\((\d+),\s*(\d+)\)', type_lower)
            if match:
                precision, scale = int(match.group(1)), int(match.group(2))
                return DecimalType(precision, scale)
            return DecimalType(38, 10)
        
        return self.TYPE_MAPPING.get(type_lower, StringType())
    
    def preserve_struct_as_json(
        self,
        df: DataFrame,
        columns_to_preserve: Optional[List[str]] = None,
        include_undefined_complex: bool = False
    ) -> DataFrame:
        """
        Convert struct/array columns to JSON strings.

        If `columns_to_preserve` is not provided, columns are inferred from
        `expected_schema` where target type is `string` and current type is
        struct or array.
        """
        if columns_to_preserve is None:
            # Auto-detect: find columns defined as 'string' in expected_schema that are currently structs or arrays
            columns_to_preserve = []
            actual_col_lookup = {c.lower(): c for c in df.columns}
            for col_def in self.expected_schema:
                expected_col_name = col_def['name']
                actual_col_name = actual_col_lookup.get(expected_col_name.lower())
                if col_def.get('data_type', '').lower() == 'string' and actual_col_name:
                    field = df.schema[actual_col_name]
                    if isinstance(field.dataType, (StructType, ArrayType)):
                        columns_to_preserve.append(actual_col_name)

            if include_undefined_complex:
                expected_columns_ci = {col_def['name'].lower() for col_def in self.expected_schema}
                for field in df.schema.fields:
                    if field.name.lower() in expected_columns_ci:
                        continue
                    if isinstance(field.dataType, (StructType, ArrayType, MapType)):
                        columns_to_preserve.append(field.name)
        
        result_df = df
        preserved_count = 0
        actual_col_lookup = {c.lower(): c for c in result_df.columns}
        
        for col_name in dict.fromkeys(columns_to_preserve):
            actual_col_name = actual_col_lookup.get(col_name.lower(), col_name)
            if actual_col_name in result_df.columns:
                field = result_df.schema[actual_col_name]
                if isinstance(field.dataType, (StructType, ArrayType, MapType)):
                    # Convert complex types to JSON string
                    result_df = result_df.withColumn(actual_col_name, F.to_json(F.col(actual_col_name)))
                    preserved_count += 1
                    if isinstance(field.dataType, StructType):
                        type_name = "struct"
                    elif isinstance(field.dataType, ArrayType):
                        type_name = "array"
                    else:
                        type_name = "map"
                    self.logger.info(f"Preserved column '{actual_col_name}' as JSON string ({type_name} -> string)")
        
        if preserved_count > 0:
            self.logger.info(f"Preserved {preserved_count} struct/array column(s) as JSON strings")
        
        return result_df
    
    def flatten_struct_columns(self, df: DataFrame, preserve_complex_types: bool = True) -> DataFrame:
        """
        Recursively flatten struct columns into top-level columns.

        Arrays/maps are kept as-is when `preserve_complex_types=True`; otherwise
        they are serialized to JSON strings.
        """
        def _flatten(schema, prefix=''):
            cols = []
            for field in schema.fields:
                # Build the Spark dot-notation path for nested access
                dot_path = f"{prefix}.{field.name}" if prefix else field.name
                # Build the flattened alias using separator
                col_alias = f"{prefix.replace('.', self.separator)}{self.separator}{field.name}" if prefix else field.name
                
                if isinstance(field.dataType, StructType):
                    # Recurse into struct
                    cols.extend(_flatten(field.dataType, dot_path))
                elif isinstance(field.dataType, (ArrayType, MapType)):
                    if preserve_complex_types:
                        # Keep arrays/maps as-is
                        cols.append(F.col(dot_path).alias(col_alias))
                    else:
                        # Convert arrays/maps to JSON strings for SQL endpoint compatibility
                        cols.append(F.to_json(F.col(dot_path)).alias(col_alias))
                else:
                    cols.append(F.col(dot_path).alias(col_alias))
            return cols
        
        flat_cols = _flatten(df.schema)
        flattened_df = df.select(flat_cols)
        
        self.logger.info(f"Flattened {len(df.columns)} columns to {len(flattened_df.columns)} columns")
        return flattened_df
    
    def validate_schema(self, df: DataFrame, raise_on_missing_critical: bool = True) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate DataFrame columns and types against `expected_schema`.

        Returns `(is_valid, report)` and optionally raises for missing critical
        columns when `raise_on_missing_critical=True`.
        """
        if not self.expected_schema:
            self.logger.info("Schema validation skipped: no expected_schema defined")
            return True, {"status": "skipped", "reason": "no expected_schema defined"}
        
        actual_columns = set(df.columns)
        expected_columns = set(self._schema_lookup.keys())
        
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        # Check for critical missing columns
        critical_missing = [
            col['name'] for col in self.expected_schema 
            if col.get('critical', False) and col['name'] not in actual_columns
        ]
        
        # Check data types
        type_mismatches = []
        for col_def in self.expected_schema:
            col_name = col_def['name']
            if col_name in actual_columns:
                expected_type = col_def.get('data_type', 'string').lower()
                actual_field = df.schema[col_name]
                actual_type = actual_field.dataType.simpleString().lower()
                
                # Normalize type names for comparison
                expected_normalized = expected_type.replace('integer', 'int').replace('bigint', 'long')
                actual_normalized = actual_type.replace('integer', 'int').replace('bigint', 'long')
                
                if expected_normalized != actual_normalized:
                    type_mismatches.append({
                        'column': col_name,
                        'expected': expected_type,
                        'actual': actual_type
                    })
        
        validation_report = {
            'is_valid': len(critical_missing) == 0,
            'missing_columns': sorted(missing_columns),
            'critical_missing': critical_missing,
            'extra_columns': sorted(extra_columns),
            'type_mismatches': type_mismatches,
            'total_expected': len(expected_columns),
            'total_actual': len(actual_columns)
        }
        
        if critical_missing and raise_on_missing_critical:
            msg = f"Schema validation FAILED - Critical column(s) missing: {critical_missing}"
            self.logger.error(msg)
            raise SchemaValidationError(msg)
        
        if missing_columns:
            self.logger.warning(f"Non-critical columns missing (will be added as null): {sorted(missing_columns - set(critical_missing))}")
        
        if extra_columns:
            self.logger.warning(f"Schema evolution: {len(extra_columns)} new column(s) will be included: {sorted(extra_columns)}")
        
        if type_mismatches:
            self.logger.warning(f"Type mismatches detected (will be converted): {type_mismatches}")
        else:
            self.logger.info("Schema validation passed")
        
        return validation_report['is_valid'], validation_report
    
    def convert_to_expected_types(self, df: DataFrame) -> DataFrame:
        """
        Cast DataFrame columns to types defined in `expected_schema`.
        """
        if not self.expected_schema:
            self.logger.info("Type conversion skipped: no expected_schema defined")
            return df
        
        result_df = df
        conversions_made = []
        actual_col_lookup = {c.lower(): c for c in df.columns}
        
        for col_def in self.expected_schema:
            col_name = col_def['name']
            actual_col_name = actual_col_lookup.get(col_name.lower())
            expected_type_str = col_def.get('data_type', 'string')
            
            if actual_col_name:
                actual_field = df.schema[actual_col_name]
                expected_type = self._get_spark_type(expected_type_str)
                
                # Check if conversion is needed
                if actual_field.dataType.simpleString().lower() != expected_type.simpleString().lower():
                    try:
                        result_df = result_df.withColumn(actual_col_name, F.col(actual_col_name).cast(expected_type))
                        conversions_made.append({
                            'column': actual_col_name,
                            'from': actual_field.dataType.simpleString(),
                            'to': expected_type.simpleString()
                        })
                    except Exception as e:
                        self.logger.warning(f"Failed to convert {actual_col_name} to {expected_type_str}: {e}")
        
        if conversions_made:
            self.logger.info(f"Converted {len(conversions_made)} columns: {conversions_made}")
        
        return result_df
    
    def add_missing_columns(self, df: DataFrame) -> DataFrame:
        """
        Add expected columns that are missing from the DataFrame.

        Added columns are null-initialized and cast to configured types.
        """
        if not self.expected_schema:
            return df
        
        result_df = df
        added_columns = []
        actual_columns_ci = {c.lower() for c in df.columns}
        
        for col_def in self.expected_schema:
            col_name = col_def['name']
            if col_name.lower() not in actual_columns_ci:
                expected_type = self._get_spark_type(col_def.get('data_type', 'string'))
                result_df = result_df.withColumn(col_name, F.lit(None).cast(expected_type))
                added_columns.append(col_name)
        
        if added_columns:
            self.logger.info(f"Added {len(added_columns)} missing columns with null values: {added_columns}")
        
        return result_df
    
    def process_dataframe(self, df: DataFrame, flatten: bool = True, validate: bool = True, 
                         convert_types: bool = True, add_missing: bool = True,
                         preserve_complex_types: bool = True, preserve_structs: bool = True,
                         allow_undefined_columns_as_string: bool = False) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Run the end-to-end processing pipeline for a DataFrame.

        Steps are controlled by flags: preserve nested structs, flatten,
        validate, add missing columns, and cast types.
        """
        report = {
            'original_columns': df.columns,
            'steps_performed': []
        }
        
        result_df = df
        
        # Step 0: Preserve struct columns defined as strings (before flattening)
        if preserve_structs and self.expected_schema:
            result_df = self.preserve_struct_as_json(
                result_df,
                include_undefined_complex=allow_undefined_columns_as_string
            )
            report['steps_performed'].append('preserve_structs')
        
        # Step 1: Flatten structs
        if flatten:
            result_df = self.flatten_struct_columns(result_df, preserve_complex_types=preserve_complex_types)
            report['steps_performed'].append('flatten')
            report['flattened_columns'] = result_df.columns
        
        # Step 2: Validate schema (only validates critical columns)
        if validate and self.expected_schema:
            is_valid, validation_report = self.validate_schema(result_df, raise_on_missing_critical=True)
            report['validation'] = validation_report
            report['steps_performed'].append('validate')
        
        # Step 3: Add missing columns (non-critical columns filled with nulls)
        if add_missing:
            result_df = self.add_missing_columns(result_df)
            report['steps_performed'].append('add_missing')
        
        # Step 4: Convert types
        if convert_types and self.expected_schema:
            result_df = self.convert_to_expected_types(result_df)
            report['steps_performed'].append('convert_types')
        
        report['final_columns'] = result_df.columns
        report['final_schema'] = [(f.name, f.dataType.simpleString(), f.nullable) for f in result_df.schema.fields]
        
        return result_df, report
    
    @classmethod
    def from_option_settings(cls, option_settings: Dict[str, Any], separator: str = '_') -> 'JsonFileHandler':
        """
        Build a handler from config option settings.

        Supports both legacy `expected_columns` list-of-strings and current
        list-of-dicts schema format.
        """
        expected_columns = option_settings.get('expected_columns', [])
        
        # Handle both old format (list of strings) and new format (list of dicts)
        if expected_columns and isinstance(expected_columns[0], str):
            # Convert old format to new format with default types
            expected_schema = [
                {'name': col, 'data_type': 'string', 'nullable': True, 'critical': False}
                for col in expected_columns
            ]
        else:
            expected_schema = expected_columns
        
        return cls(expected_schema=expected_schema, separator=separator)


class SchemaValidationError(Exception):
    """Raised when DataFrame schema validation fails for critical columns."""
    pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
