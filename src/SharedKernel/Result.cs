namespace SharedKernel;

/// <summary>Represents operation result without value.</summary>
public class Result
{
    /// <summary>Whether operation succeeded.</summary>
    public bool IsSuccess { get; }
    /// <summary>Error message if failed.</summary>
    public string? Error { get; }
    /// <summary>Creates result.</summary>
    protected Result(bool isSuccess, string? error) { IsSuccess = isSuccess; Error = error; }
    /// <summary>Success result.</summary>
    public static Result Success() => new(true, null);
    /// <summary>Failure result.</summary>
    public static Result Failure(string error) => new(false, error);
}
/// <summary>Represents operation result with value.</summary>
public class Result<T> : Result
{
    /// <summary>Value if success.</summary>
    public T? Value { get; }
    private Result(T? value, bool isSuccess, string? error) : base(isSuccess, error) { Value = value; }
    /// <summary>Success with value.</summary>
    public static Result<T> Success(T value) => new(value, true, null);
    /// <summary>Failure with error.</summary>
    public static new Result<T> Failure(string error) => new(default, false, error);
}
