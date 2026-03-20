use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro_crate::{crate_name, FoundCrate};
use quote::{format_ident, quote};
use syn::{parse_macro_input, FnArg, ItemFn, Pat, Type};

/// Returns the token path to the `resonate` crate root.
/// - When compiled inside the `resonate` crate itself → `crate`
/// - When compiled by an external consumer          → `resonate` (or the renamed alias)
fn resonate_crate() -> TokenStream2 {
    match crate_name("resonate") {
        Ok(FoundCrate::Itself) => quote! { crate },
        Ok(FoundCrate::Name(name)) => {
            let ident = format_ident!("{}", name);
            quote! { #ident }
        }
        // Fallback: assume external usage
        Err(_) => quote! { resonate },
    }
}

/// Attribute macro for registering durable functions.
///
/// Detects the function kind from the first parameter's type:
/// - `&Context` → Workflow
/// - `&Info` → Leaf with metadata
/// - anything else → Pure leaf
///
/// Generates a unit struct implementing the `Durable` trait.
///
/// # Examples
///
/// ```ignore
/// #[resonate::function]
/// async fn my_leaf(x: i32) -> Result<i32> { Ok(x + 1) }
///
/// #[resonate::function]
/// async fn my_workflow(ctx: &Context, x: i32) -> Result<i32> {
///     ctx.run(MyLeaf, x).await
/// }
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

    // Resolve the crate path (crate:: when inside resonate, resonate:: externally)
    let krate = resonate_crate();

    // Generate the kind constant
    let kind_token = match kind {
        FunctionKind::PureLeaf | FunctionKind::LeafWithInfo => {
            quote! { #krate::types::DurableKind::Function }
        }
        FunctionKind::Workflow => quote! { #krate::types::DurableKind::Workflow },
    };

    // Generate the execute body based on kind
    let execute_body = match kind {
        FunctionKind::PureLeaf => {
            if arg_names.is_empty() {
                quote! {
                    #fn_name().await
                }
            } else if arg_names.len() == 1 {
                quote! {
                    #fn_name(args).await
                }
            } else {
                let destructure: Vec<_> = arg_names.iter().enumerate().map(|(i, name)| {
                    let idx = syn::Index::from(i);
                    quote! { let #name = args.#idx; }
                }).collect();
                quote! {
                    #(#destructure)*
                    #fn_name(#(#arg_names),*).await
                }
            }
        }
        FunctionKind::LeafWithInfo => {
            let info_unwrap = quote! {
                let info = info.expect("Info must be provided for leaf+info functions");
            };
            if arg_names.len() == 1 {
                quote! {
                    #info_unwrap
                    #fn_name(info, args).await
                }
            } else {
                let destructure: Vec<_> = arg_names.iter().enumerate().map(|(i, name)| {
                    let idx = syn::Index::from(i);
                    quote! { let #name = args.#idx; }
                }).collect();
                quote! {
                    #info_unwrap
                    #(#destructure)*
                    #fn_name(info, #(#arg_names),*).await
                }
            }
        }
        FunctionKind::Workflow => {
            let ctx_unwrap = quote! {
                let ctx = ctx.expect("Context must be provided for workflow functions");
            };
            if arg_names.len() == 1 {
                quote! {
                    #ctx_unwrap
                    #fn_name(ctx, args).await
                }
            } else {
                let destructure: Vec<_> = arg_names.iter().enumerate().map(|(i, name)| {
                    let idx = syn::Index::from(i);
                    quote! { let #name = args.#idx; }
                }).collect();
                quote! {
                    #ctx_unwrap
                    #(#destructure)*
                    #fn_name(ctx, #(#arg_names),*).await
                }
            }
        }
    };

    let output = quote! {
        // Keep the original function
        #input

        /// Generated durable function struct for `#fn_name`.
        #[derive(Debug, Clone, Copy)]
        #vis struct #struct_name;

        impl #krate::durable::Durable<#args_type, #return_type> for #struct_name {
            const NAME: &'static str = #registered_name;
            const KIND: #krate::types::DurableKind = #kind_token;

            async fn execute(
                &self,
                ctx: Option<&#krate::context::Context>,
                info: Option<&#krate::info::Info>,
                args: #args_type,
            ) -> #krate::error::Result<#return_type> {
                #execute_body
            }
        }
    };

    Ok(output)
}

/// Detect the function kind by inspecting the first parameter's type.
fn detect_kind<'a>(
    params: &'a [&'a FnArg],
) -> syn::Result<(FunctionKind, Vec<&'a FnArg>)> {
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
        syn::ReturnType::Default => {
            Ok(quote! { () })
        }
        syn::ReturnType::Type(_, ty) => {
            // Try to extract T from Result<T>
            if let Type::Path(type_path) = ty.as_ref() {
                if let Some(segment) = type_path.path.segments.last() {
                    if segment.ident == "Result" {
                        if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                            if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first()
                            {
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
