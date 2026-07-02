pub mod loader;
pub mod runtime;

pub use loader::register;
pub use runtime::{call, call_plugin};

#[cfg(test)]
mod tests {
    use super::*;

    fn pass_through_wasm() -> Vec<u8> {
        wat::parse_str(
            r#"
(module
  (memory (export "memory") 1)
  (func (export "process") (param $ptr i32) (param $len i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len))
    )
  )
)
"#,
        )
        .expect("wat parse failed")
    }

    fn echo_json_wasm() -> Vec<u8> {
        wat::parse_str(
            r#"
(module
  (memory (export "memory") 1)
  (func (export "echo_json") (param $ptr i32) (param $len i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len))
    )
  )
)
"#,
        )
        .expect("wat parse failed")
    }

    #[test]
    fn test_parse_expr_w_dot() {
        let result = call_plugin("w:myplugin.process", b"{}");
        assert!(result.is_err(), "expected error for unregistered plugin");
        let err = result.unwrap_err();
        assert!(
            err.contains("not registered"),
            "expected 'not registered' error, got: {}",
            err
        );
    }

    #[test]
    fn test_parse_expr_w_dot_with_parens() {
        let result = call_plugin("w:myplugin.process()", b"{}");
        assert!(result.is_err(), "expected error for unregistered plugin");
        let err = result.unwrap_err();
        assert!(
            err.contains("not registered"),
            "expected 'not registered' error, got: {}",
            err
        );
    }

    #[test]
    fn test_register_and_call_missing_func() {
        register("test_plugin", b"\x00asm\x01\x00\x00\x00");
        let result = call("test_plugin", "nonexistent", b"{}");
        assert!(result.is_err(), "expected error for invalid wasm bytes");
    }

    #[test]
    fn test_register_duplicate_overwrites() {
        register("dup", b"old bytes");
        register("dup", b"new bytes");
        assert!(loader::PLUGIN_BYTES.lock().unwrap().get("dup").unwrap().len() > 0);
    }

    #[test]
    fn test_call_pass_through_plugin() {
        let wasm = pass_through_wasm();
        register("passthru", &wasm);
        let input = br#"{"hello":"world"}"#;
        let result = call("passthru", "process", input).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn test_call_plugin_via_call_plugin() {
        let wasm = echo_json_wasm();
        register("echo", &wasm);
        let input = br#"{"x":42}"#;
        let result = call_plugin("w:echo.echo_json", input).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn test_plugin_integration_through_vm() {
        let wasm = pass_through_wasm();
        register("sig", &wasm);

        let plan = crate::dsl::compiler::Compiler::new()
            .compile(
                &crate::dsl::optimizer::Optimizer::new().optimize(
                    &crate::dsl::parser::parse(
                        &crate::dsl::lexer::lex("w:sig.process n:svc").unwrap(),
                    )
                    .unwrap(),
                ),
                "test",
            )
            .unwrap();

        let arena = crate::memory::arena::Arena::new();
        let ctx = crate::bytecode::execution::ExecutionContext::from_body(br#"{"msg":"hi"}"#.to_vec());
        let mut vm = crate::executor::VM::new(
            &plan,
            ctx,
            arena,
            &|_svc_id: u16, body: &[u8], _timeout: u64| Ok(body.to_vec()),
        );
        vm.run().unwrap();
        assert_eq!(vm.ctx.hop_count, 1);
        let output: serde_json::Value =
            serde_json::from_slice(&vm.ctx.body).unwrap();
        assert_eq!(output["msg"], "hi");
    }
}
