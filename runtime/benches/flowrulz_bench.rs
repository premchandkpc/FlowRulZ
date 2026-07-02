use criterion::{black_box, criterion_group, criterion_main, Criterion};

use flowrulz_core::bytecode::execution::ExecutionContext;
use flowrulz_core::bytecode::plan::ExecutionPlan;
use flowrulz_core::dsl::{compiler, lexer, optimizer, parser};

fn compile_plan(dsl: &str) -> ExecutionPlan {
    let tokens = lexer::lex(dsl).unwrap();
    let pipeline = parser::parse(&tokens).unwrap();
    let opt = optimizer::Optimizer::new();
    let optimized = opt.optimize(&pipeline);
    let comp = compiler::Compiler::new();
    comp.compile(&optimized, "bench").unwrap()
}

fn bench_compile(c: &mut Criterion) {
    let dsles = [
        "n:validate",
        "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics",
        "g:amount>10000 n:manual-review | t300 n:auto-approve f:hold-queue",
        "dag:{A:[B,C],D:[A]} e:audit",
        "chunk:10:par n:storage r3:exp",
    ];

    let mut group = c.benchmark_group("compile");
    for (i, dsl) in dsles.iter().enumerate() {
        group.bench_with_input(format!("dsl_{}", i), dsl, |b, d| {
            b.iter(|| {
                let tokens = lexer::lex(black_box(d)).unwrap();
                let pipeline = parser::parse(&tokens).unwrap();
                let opt = optimizer::Optimizer::new();
                let optimized = opt.optimize(&pipeline);
                let comp = compiler::Compiler::new();
                comp.compile(&optimized, "bench").unwrap()
            });
        });
    }
    group.finish();
}

fn bench_vm_execute(c: &mut Criterion) {
    let plan = compile_plan("t500 n:validate");

    let mut group = c.benchmark_group("vm_execute");
    group.bench_with_input("simple_next", &plan, |b, plan| {
        b.iter(|| {
            let body = b"{\"type\":\"ORDER\",\"amount\":500}";
            let arena = flowrulz_core::memory::arena::Arena::new();
            let caller = |_svc_id: u16, body: &[u8], _timeout: u64| Ok(body.to_vec());
            let ctx = ExecutionContext::from_body(black_box(body).to_vec());
            let mut vm = flowrulz_core::executor::VM::new(plan, ctx, arena, &caller);
            vm.run().unwrap();
        });
    });
    group.finish();
}

fn bench_full_pipeline(c: &mut Criterion) {
    let dsl = "t500 n:validate t1000 p:fraud,inventory c f:dlq n:fulfill e:notify,analytics";
    let plan = compile_plan(dsl);

    let mut group = c.benchmark_group("full_pipeline");
    group.bench_with_input("standard", &plan, |b, plan| {
        b.iter(|| {
            let body = b"{\"type\":\"ORDER\",\"amount\":500,\"user\":\"alice\"}";
            let arena = flowrulz_core::memory::arena::Arena::new();
            let caller = |_svc_id: u16, body: &[u8], _timeout: u64| Ok(body.to_vec());
            let ctx = ExecutionContext::from_body(black_box(body).to_vec());
            let mut vm = flowrulz_core::executor::VM::new(plan, ctx, arena, &caller);
            vm.run().unwrap();
        });
    });
    group.finish();
}

fn bench_gate_eval(c: &mut Criterion) {
    let dsl = "g:amount>10000 n:validate";
    let plan = compile_plan(dsl);

    let mut group = c.benchmark_group("gate_eval");
    group.bench_with_input("true_branch", &plan, |b, plan| {
        b.iter(|| {
            let body = b"{\"amount\":15000}";
            let arena = flowrulz_core::memory::arena::Arena::new();
            let caller = |_svc_id: u16, body: &[u8], _timeout: u64| Ok(body.to_vec());
            let ctx = ExecutionContext::from_body(black_box(body).to_vec());
            let mut vm = flowrulz_core::executor::VM::new(plan, ctx, arena, &caller);
            vm.run().unwrap();
        });
    });
    group.finish();
}

fn bench_dag(c: &mut Criterion) {
    let dsl = "dag:{A:[B,C],D:[A]} e:audit";
    let plan = compile_plan(dsl);

    let mut group = c.benchmark_group("dag");
    group.bench_with_input("4_node", &plan, |b, plan| {
        b.iter(|| {
            let body = b"{\"x\":1}";
            let arena = flowrulz_core::memory::arena::Arena::new();
            let caller = |_svc_id: u16, body: &[u8], _timeout: u64| Ok(body.to_vec());
            let ctx = ExecutionContext::from_body(black_box(body).to_vec());
            let mut vm = flowrulz_core::executor::VM::new(plan, ctx, arena, &caller);
            vm.run().unwrap();
        });
    });
    group.finish();
}

criterion_group!(benches, bench_compile, bench_vm_execute, bench_full_pipeline, bench_gate_eval, bench_dag);
criterion_main!(benches);
