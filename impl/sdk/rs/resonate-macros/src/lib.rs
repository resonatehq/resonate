use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{format_ident, quote};
use syn::{parse_macro_input, FnArg, ItemFn, Pat, Type};

/// Returns the token path to the `resonate-sdk` crate root.
/// - When compiled inside the `resonate-sdk` crate itself → `crate`
/// - When compiled by an external consumer               → `resonate_sdk` (or the renamed alias)
fn resonate_crate() -> TokenStream2 {
    match crate_name("resonate-sdk") {
        Ok(FoundCrate::Itself) => quote! { crate },
        Ok(FoundCrate::Name(name)) => {
            let ident = format_ident!("{}", name);
            quote! { #ident }
        }
        // Fallback: assume external usage
        Err(_) => quote! { resonate_sdk },
    }
}

/// Attribute macro for registering durable functions.
///
/// Detects the function kind from the first parameter's type:
/// - `&Context` → Workflow
/// - `&Info` → Leaf with metadata
/// - anything else → Pure leaf
///
/// Generates a PascalCase unit struct implementing `Durable`, plus a
/// lowercase const alias matching the original function name. The original
/// function is consumed — its body is inlined into the `Durable::execute` impl.
///
/// # Examples
///
/// ```ignore
/// #[resonate_sdk::function]
/// async fn my_leaf(x: i32) -> Result<i32> { Ok(x + 1) }
///
/// #[resonate_sdk::function]
/// async fn my_workflow(ctx: &Context, x: i32) -> Result<i32> {
///     ctx.run(my_leaf, x).await
/// }
///
/// // Both lowercase const and PascalCase struct work:
/// ctx.run(my_leaf, 42).await     // preferred
/// ctx.run(MyLeaf, 42).await      // also works
/// resonate.register(my_leaf)     // preferred
/// resonate.register(MyLeaf)      // also works
/// ```
#[proc_macro_attribute]
pub fn function(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let attrs = if attr.is_empty() {
        None
    } else {
        Some(parse_macro_input!(attr as MacroAttrs))
    };

    match generate_durable_impl(input, attrs) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Parsed macro attributes.
struct MacroAttrs {
    name: Option<String>,
}

impl syn::parse::Parse for MacroAttrs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut name = None;

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            let _eq: syn::Token![=] = input.parse()?;

            if ident == "name" {
                let lit: syn::LitStr = input.parse()?;
                name = Some(lit.value());
            } else {
                return Err(syn::Error::new(ident.span(), "unknown attribute"));
            }

            if input.peek(syn::Token![,]) {
                let _comma: syn::Token![,] = input.parse()?;
            }
        }

        Ok(MacroAttrs { name })
    }
}

/// The detected kind of function based on its first parameter.
enum FunctionKind {
    /// Pure leaf: no special first argument
    PureLeaf,
    /// Leaf with Info: first argument is `&Info`
    LeafWithInfo,
    /// Workflow: first argument is `&Context`
    Workflow,
}

fn generate_durable_impl(
    input: ItemFn,
    attrs: Option<MacroAttrs>,
) -> syn::Result<proc_macro2::TokenStream> {
    let fn_name = &input.sig.ident;
    let vis = &input.vis;

    // Generate struct name: snake_case → PascalCase
    let struct_name = format_ident!("{}", to_pascal_case(&fn_name.to_string()));

    // Determine the registered name
    let registered_name = attrs
        .and_then(|a| a.name)
        .unwrap_or_else(|| fn_name.to_string());

    // Analyze the function signature to detect the kind
    let params: Vec<_> = input.sig.inputs.iter().collect();
    let (kind, user_params) = detect_kind(&params)?;

    // Extract user parameter types and names
    let mut arg_types = Vec::new();
    let mut arg_names = Vec::new();

    for param in &user_params {
        if let FnArg::Typed(pat_type) = param {
            arg_types.push(pat_type.ty.as_ref().clone());
            if let Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                arg_names.push(pat_ident.ident.clone());
            } else {
                return Err(syn::Error::new_spanned(
                    pat_type,
                    "expected a simple identifier pattern",
                ));
            }
        }
    }

    // Determine the Args and T types
    // For the Durable trait, Args is a tuple of all user params, T is extracted from Result<T>
    let args_type = if arg_types.is_empty() {
        quote! { () }
    } else if arg_types.len() == 1 {
        let t = &arg_types[0];
        quote! { #t }
    } else {
        quote! { (#(#arg_types),*) }
    };

    // Extract return type T from Result<T>
    let return_type = extract_return_type(&input.sig.output)?;

    // Resolve the crate path (crate:: when inside resonate-sdk, resonate_sdk:: externally)
    let krate = resonate_crate();

    // Generate the kind constant
    let kind_token = match kind {
        FunctionKind::PureLeaf | FunctionKind::LeafWithInfo => {
            quote! { #krate::types::DurableKind::Function }
        }
        FunctionKind::Workflow => quote! { #krate::types::DurableKind::Workflow },
    };

    // Extract the function body and any attributes (e.g. #[allow(...)])
    let fn_body = &input.block;
    let fn_attrs = &input.attrs;

    // Generate the execute body based on kind.
    // Instead of delegating to the original function, we inline the body directly.
    // We destructure the `args` tuple and set up `ctx`/`info` bindings as needed.
    let execute_body = match kind {
        FunctionKind::PureLeaf => {
            let ignore_env = quote! { let _ = env; };
            if arg_names.is_empty() {
                quote! {
                    #ignore_env
                    async move #fn_body .await
                }
            } else if arg_names.len() == 1 {
                let name = &arg_names[0];
                quote! {
                    #ignore_env
                    let #name = args;
                    async move #fn_body .await
                }
            } else {
                let destructure: Vec<_> = arg_names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        let idx = syn::Index::from(i);
                        quote! { let #name = args.#idx; }
                    })
                    .collect();
                quote! {
                    #ignore_env
                    #(#destructure)*
                    async move #fn_body .await
                }
            }
        }
        FunctionKind::LeafWithInfo => {
            let info_unwrap = quote! {
                let info = env.into_info();
            };
            if arg_names.is_empty() {
                quote! {
                    #info_unwrap
                    async move #fn_body .await
                }
            } else if arg_names.len() == 1 {
                let name = &arg_names[0];
                quote! {
                    #info_unwrap
                    let #name = args;
                    async move #fn_body .await
                }
            } else {
                let destructure: Vec<_> = arg_names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        let idx = syn::Index::from(i);
                        quote! { let #name = args.#idx; }
                    })
                    .collect();
                quote! {
                    #info_unwrap
                    #(#destructure)*
                    async move #fn_body .await
                }
            }
        }
        FunctionKind::Workflow => {
            let ctx_unwrap = quote! {
                let ctx = env.into_context();
            };
            if arg_names.is_empty() {
                quote! {
                    #ctx_unwrap
                    async move #fn_body .await
                }
            } else if arg_names.len() == 1 {
                let name = &arg_names[0];
                quote! {
                    #ctx_unwrap
                    let #name = args;
                    async move #fn_body .await
                }
            } else {
                let destructure: Vec<_> = arg_names
                    .iter()
                    .enumerate()
                    .map(|(i, name)| {
                        let idx = syn::Index::from(i);
                        quote! { let #name = args.#idx; }
                    })
                    .collect();
                quote! {
                    #ctx_unwrap
                    #(#destructure)*
                    async move #fn_body .await
                }
            }
        }
    };

    let internal_struct_name = format_ident!("__Durable_{}", struct_name);

    let output = quote! {
        /// Generated durable function struct (internal — use the const `#fn_name` instead).
        #(#fn_attrs)*
        #[derive(Debug, Clone, Copy)]
        #[doc(hidden)]
        #vis struct #internal_struct_name;

        /// Durable function handle. Use with `ctx.run(#fn_name, args)` or `resonate.register(#fn_name)`.
        #[allow(non_upper_case_globals)]
        #vis const #fn_name: #internal_struct_name = #internal_struct_name;

        impl #krate::durable::Durable<#args_type, #return_type> for #internal_struct_name {
            const NAME: &'static str = #registered_name;
            const KIND: #krate::types::DurableKind = #kind_token;

            async fn execute(
                &self,
                env: #krate::durable::ExecutionEnv<'_>,
                args: #args_type,
            ) -> #krate::error::Result<#return_type> {
                #execute_body
            }
        }
    };

    Ok(output)
}

/// Detect the function kind by inspecting the first parameter's type.
fn detect_kind<'a>(params: &'a [&'a FnArg]) -> syn::Result<(FunctionKind, Vec<&'a FnArg>)> {
    if params.is_empty() {
        return Ok((FunctionKind::PureLeaf, vec![]));
    }

    // Check the first parameter's type
    if let FnArg::Typed(pat_type) = params[0] {
        if is_reference_to(&pat_type.ty, "Context") {
            return Ok((FunctionKind::Workflow, params[1..].to_vec()));
        }
        if is_reference_to(&pat_type.ty, "Info") {
            return Ok((FunctionKind::LeafWithInfo, params[1..].to_vec()));
        }
    }

    // No special first param — pure leaf
    Ok((FunctionKind::PureLeaf, params.to_vec()))
}

/// Check if a type is a reference to a type with the given name.
fn is_reference_to(ty: &Type, name: &str) -> bool {
    if let Type::Reference(type_ref) = ty {
        if let Type::Path(type_path) = type_ref.elem.as_ref() {
            if let Some(segment) = type_path.path.segments.last() {
                return segment.ident == name;
            }
        }
    }
    false
}

/// Extract the T from `-> Result<T>` return type.
fn extract_return_type(output: &syn::ReturnType) -> syn::Result<proc_macro2::TokenStream> {
    match output {
        syn::ReturnType::Default => Ok(quote! { () }),
        syn::ReturnType::Type(_, ty) => {
            // Try to extract T from Result<T>
            if let Type::Path(type_path) = ty.as_ref() {
                if let Some(segment) = type_path.path.segments.last() {
                    if segment.ident == "Result" {
                        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                            if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                                return Ok(quote! { #inner_ty });
                            }
                        }
                    }
                }
            }
            // If we can't extract, use the full type
            Ok(quote! { #ty })
        }
    }
}

/// Convert snake_case to PascalCase.
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}
