#![cfg_attr(all(target_arch = "wasm32", not(feature = "bindgen")), no_std)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::ffi::c_void;
use core::mem;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct TraceSpan {
    #[serde(rename = "request_id")]
    request_id: Option<String>,
    #[serde(rename = "trace_id")]
    trace_id: Option<String>,
    #[serde(rename = "span_id")]
    span_id: Option<String>,
    #[serde(rename = "function_name")]
    function_name: Option<String>,
    #[serde(rename = "service_name")]
    service_name: Option<String>,
    #[serde(rename = "duration_ms")]
    duration_ms: Option<i64>,
    #[serde(rename = "status_code")]
    status_code: Option<i32>,
    #[serde(rename = "method")]
    method: Option<String>,
    #[serde(rename = "path")]
    path: Option<String>,
    #[serde(rename = "tags")]
    tags: Option<alloc::collections::BTreeMap<String, String>>,
}

#[link(wasm_import_module = "env")]
extern "C" {
    #[link_name = "log"]
    fn host_log(ptr: i32, len: i32);

    #[link_name = "get_config"]
    fn host_get_config(ptr: i32) -> i32;

    #[link_name = "set_result"]
    fn host_set_result(keep: i32, reason_ptr: i32, reason_len: i32);
}

struct PluginConfig {
    slow_threshold_ms: i64,
    always_keep_errors: bool,
    min_error_code: i32,
    ignore_paths: Vec<String>,
}

impl PluginConfig {
    fn default() -> Self {
        PluginConfig {
            slow_threshold_ms: 200,
            always_keep_errors: true,
            min_error_code: 400,
            ignore_paths: Vec::new(),
        }
    }

    fn parse(config: &str) -> Self {
        let mut cfg = Self::default();
        for line in config.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim();
                let value = value.trim();
                match key {
                    "slow_threshold_ms" => {
                        if let Ok(v) = value.parse::<i64>() {
                            cfg.slow_threshold_ms = v;
                        }
                    }
                    "always_keep_errors" => {
                        cfg.always_keep_errors = value.eq_ignore_ascii_case("true") || value == "1";
                    }
                    "min_error_code" => {
                        if let Ok(v) = value.parse::<i32>() {
                            cfg.min_error_code = v;
                        }
                    }
                    "ignore_paths" => {
                        cfg.ignore_paths = value
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                    }
                    _ => {}
                }
            }
        }
        cfg
    }
}

static mut CONFIG: Option<Box<PluginConfig>> = None;
static mut MEMORY: Vec<u8> = Vec::new();

fn wasm_log(msg: &str) {
    let bytes = msg.as_bytes();
    let ptr = bytes.as_ptr() as i32;
    let len = bytes.len() as i32;
    unsafe {
        host_log(ptr, len);
    }
}

fn load_config() -> Box<PluginConfig> {
    unsafe {
        if let Some(cfg) = &CONFIG {
            return cfg.clone();
        }

        let buf_ptr = Box::into_raw(Box::new([0u8; 4096])) as i32;
        let config_ptr = host_get_config(buf_ptr);

        if config_ptr == 0 {
            let default_cfg = Box::new(PluginConfig::default());
            CONFIG = Some(default_cfg.clone());
            return default_cfg;
        }

        let slice = core::slice::from_raw_parts(config_ptr as *const u8, 4096);
        let mut len = 0;
        while len < 4096 && slice[len] != 0 {
            len += 1;
        }

        let config_str = core::str::from_utf8(&slice[..len]).unwrap_or("");
        let cfg = Box::new(PluginConfig::parse(config_str));

        let _ = Box::from_raw(buf_ptr as *mut [u8; 4096]);

        wasm_log(&alloc::format!(
            "[slow-request-filter] loaded config: threshold={}ms, keep_errors={}",
            cfg.slow_threshold_ms,
            cfg.always_keep_errors
        ));

        CONFIG = Some(cfg.clone());
        cfg
    }
}

#[no_mangle]
pub extern "C" fn init() -> i32 {
    wasm_log("[slow-request-filter] Initializing slow request filter...");
    let cfg = load_config();
    wasm_log(&alloc::format!(
        "[slow-request-filter] Ready. Threshold: {}ms, Keep errors: {}",
        cfg.slow_threshold_ms,
        cfg.always_keep_errors
    ));
    0
}

#[no_mangle]
pub extern "C" fn dealloc() -> i32 {
    wasm_log("[slow-request-filter] Deallocating");
    unsafe {
        CONFIG = None;
    }
    0
}

#[no_mangle]
pub extern "C" fn alloc(size: i32) -> i32 {
    let size = size as usize;
    unsafe {
        MEMORY.resize(MEMORY.len() + size, 0);
        (MEMORY.as_ptr() as usize + MEMORY.len() - size) as i32
    }
}

#[no_mangle]
pub extern "C" fn free(_ptr: i32) -> i32 {
    0
}

#[no_mangle]
pub extern "C" fn filter_span(span_ptr: i32, span_len: i32) -> i32 {
    let span_len = span_len as usize;

    let span_json: String = if span_ptr != 0 && span_len > 0 {
        unsafe {
            let slice = core::slice::from_raw_parts(span_ptr as *const u8, span_len);
            String::from_utf8_lossy(slice).to_string()
        }
    } else {
        return 1;
    };

    let span: TraceSpan = match serde_json::from_str(&span_json) {
        Ok(s) => s,
        Err(_e) => {
            return 1;
        }
    };

    let cfg = load_config();

    if let Some(status_code) = span.status_code {
        if cfg.always_keep_errors && status_code >= cfg.min_error_code {
            return 1;
        }
    }

    if let Some(path) = &span.path {
        for ignore in &cfg.ignore_paths {
            if path.contains(ignore.as_str()) {
                return 0;
            }
        }
    }

    let duration = span.duration_ms.unwrap_or(0);
    if duration >= cfg.slow_threshold_ms {
        return 1;
    }

    let mut should_keep = false;
    if let Some(tags) = &span.tags {
        if let Some(_) = tags.get("priority") {
            should_keep = true;
        }
        if let Some(v) = tags.get("sample") {
            if v.eq_ignore_ascii_case("true") {
                should_keep = true;
            }
        }
    }

    if should_keep {
        return 1;
    }

    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = PluginConfig::default();
        assert_eq!(cfg.slow_threshold_ms, 200);
        assert!(cfg.always_keep_errors);
        assert_eq!(cfg.min_error_code, 400);
    }

    #[test]
    fn test_config_parse() {
        let cfg = PluginConfig::parse(
            "slow_threshold_ms=500\nalways_keep_errors=false\nmin_error_code=500",
        );
        assert_eq!(cfg.slow_threshold_ms, 500);
        assert!(!cfg.always_keep_errors);
        assert_eq!(cfg.min_error_code, 500);
    }
}
