#pragma once

enum class CmdPrepareResult {
    success,
    unrecognized,
    syntax_error,
    string_too_long,
    id_out_of_range
};