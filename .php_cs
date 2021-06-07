<?php

$finder = PhpCsFixer\Finder::create()->in([
    __DIR__ . '/src/',
    __DIR__ . '/config/',
    __DIR__ . '/database/',
    __DIR__ . '/tests/',
]);

return (new PhpCsFixer\Config())
    ->setRules([
        '@Symfony'                => true,
        'native_function_invocation' => ['scope' => 'all'],
        'global_namespace_import' =>[
            'import_classes' => true, 'import_constants' => true, 'import_functions' => true
        ],
        'concat_space'            => ['spacing' => 'one'],
        'declare_strict_types'    => true,
        'no_alias_functions'      => true,
        'not_operator_with_space' => true,
        'return_type_declaration' => true,
        'phpdoc_to_return_type'   => true,
        'binary_operator_spaces'  => false,
        'php_unit_method_casing'  => ['case' => 'snake_case'],
        'void_return'             => true,
        'ordered_imports'         => [
            'sort_algorithm' => 'length',
            'imports_order'  => ['const', 'class', 'function'],
        ],
    ])
    ->setRiskyAllowed(true)
    ->setFinder($finder);
