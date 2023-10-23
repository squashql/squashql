import {TableField} from "../../field";
import * as dependencies from "../../dependencies";
import {BucketColumnSet} from "../../columnsets";
import {ColumnSet} from "../../types";

afterEach(() => {
  jest.restoreAllMocks();
});

describe('computeColumnSetDependencies', () => {

  it('should compute dependencies for BucketColumnSet', () => {
    const createdField = new TableField('mockTable.createdField');
    const mockField = new TableField('mockTable.mockField');
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockField) {
        array.push(mockField);
      }
      return array;
    });

    const columnSet = new BucketColumnSet(createdField, mockField, new Map());
    const result = dependencies.computeColumnSetDependencies(columnSet);

    expect(result).toEqual(expect.arrayContaining([mockField]));
  });

  it('should throw an error for unknown ColumnSet type', () => {
    class UnknownBucketColumnSet implements ColumnSet {
      readonly class: string;
      readonly key: string;
    }

    const unknownBucketColumnSet = new UnknownBucketColumnSet();

    expect(() => dependencies.computeColumnSetDependencies(unknownBucketColumnSet)).toThrow(
            "ColumnSet with unknown type: class UnknownBucketColumnSet"
    );
  });

});
