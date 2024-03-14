using Apache.Arrow;
using Apache.Arrow.Types;

namespace ParquetSharp.Dataset;

internal sealed class TypeComparer
    : IArrowTypeVisitor
    , IArrowTypeVisitor<DictionaryType>
    , IArrowTypeVisitor<FixedSizeBinaryType>
    , IArrowTypeVisitor<IntervalType>
    , IArrowTypeVisitor<ListType>
    , IArrowTypeVisitor<StructType>
    , IArrowTypeVisitor<Time32Type>
    , IArrowTypeVisitor<Time64Type>
    , IArrowTypeVisitor<TimestampType>
    , IArrowTypeVisitor<UnionType>
{
    public TypeComparer(IArrowType expectedType)
    {
        _expectedType = expectedType;
    }

    public bool TypesMatch { get; private set; } = false;

    public void Visit(DictionaryType type)
    {
        if (_expectedType is DictionaryType expectedType)
        {
            var indexComparer = new TypeComparer(expectedType.IndexType);
            var valueComparer = new TypeComparer(expectedType.ValueType);
            type.IndexType.Accept(indexComparer);
            type.ValueType.Accept(valueComparer);
            TypesMatch = indexComparer.TypesMatch
                             && valueComparer.TypesMatch
                             && type.Ordered == expectedType.Ordered;
        }

        TypesMatch = false;
    }

    public void Visit(FixedSizeBinaryType type)
    {
        TypesMatch = _expectedType is FixedSizeBinaryType expectedType && type.ByteWidth == expectedType.ByteWidth;
    }

    public void Visit(IntervalType type)
    {
        TypesMatch = _expectedType is IntervalType expectedType && type.Unit == expectedType.Unit;
    }

    public void Visit(ListType type)
    {
        if (_expectedType is ListType expectedType)
        {
            var valueComparer = new TypeComparer(expectedType.ValueDataType);
            type.ValueDataType.Accept(valueComparer);
            TypesMatch = valueComparer.TypesMatch;
        }

        TypesMatch = false;
    }

    public void Visit(StructType type)
    {
        TypesMatch = _expectedType is StructType expectedType && FieldsMatch(type.Fields, expectedType.Fields);
    }

    public void Visit(Time32Type type)
    {
        TypesMatch = _expectedType is Time32Type expectedType && type.Unit == expectedType.Unit;
    }

    public void Visit(Time64Type type)
    {
        TypesMatch = _expectedType is Time64Type expectedType && type.Unit == expectedType.Unit;
    }

    public void Visit(TimestampType type)
    {
        TypesMatch = _expectedType is TimestampType expectedType
                     && type.Unit == expectedType.Unit
                     && type.Timezone == expectedType.Timezone;
    }

    public void Visit(UnionType type)
    {
        TypesMatch = _expectedType is UnionType expectedType
                     && type.Mode == expectedType.Mode
                     && type.TypeCodes.SequenceEqual(expectedType.TypeCodes);
    }

    public void Visit(IArrowType type)
    {
        TypesMatch = type.TypeId == _expectedType.TypeId;
    }

    private static bool FieldsMatch(IReadOnlyList<Field> actualFields, IReadOnlyList<Field> expectedFields)
    {
        if (actualFields.Count != expectedFields.Count)
        {
            return false;
        }

        for (var fieldIdx = 0; fieldIdx < expectedFields.Count; ++fieldIdx)
        {
            var actualField = actualFields[fieldIdx];
            var expectedField = expectedFields[fieldIdx];
            if (actualField.Name != expectedField.Name)
            {
                return false;
            }

            if (actualField.IsNullable != expectedField.IsNullable)
            {
                return false;
            }

            var fieldTypeComparer = new TypeComparer(expectedField.DataType);
            actualField.DataType.Accept(fieldTypeComparer);
            if (!fieldTypeComparer.TypesMatch)
            {
                return false;
            }
        }

        return true;
    }

    private readonly IArrowType _expectedType;
}
