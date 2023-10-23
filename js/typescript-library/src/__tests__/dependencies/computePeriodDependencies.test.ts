import {TableField} from "../../field";
import * as dependencies from "../../dependencies";
import {Month, Quarter, Semester, Year} from "../../columnsets";
import {Period} from "../../types";

afterEach(() => {
  jest.restoreAllMocks();
});

describe('computePeriodDependencies', () => {

  const mockYear = new TableField('mockYear');
  const mockSemester = new TableField('mockSemester');
  const mockQuarter = new TableField('mockQuarter');
  const mockMonth = new TableField('mockMonth');

  it('should compute dependencies for Year', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockYear) {
        array.push(mockYear);
      }
      return array;
    });

    const period = new Year(mockYear);
    const result = dependencies.computePeriodDependencies(period);

    expect(result).toEqual(expect.arrayContaining([mockYear]));
  });

  it('should compute dependencies for Semester', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockSemester) {
        array.push(mockSemester);
      } else if (field === mockYear) {
        array.push(mockYear);
      }
      return array;
    });

    const period = new Semester(mockSemester, mockYear);
    const result = dependencies.computePeriodDependencies(period);

    expect(result).toEqual(expect.arrayContaining([mockSemester, mockYear]));
  });

  it('should compute dependencies for Quarter', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockQuarter) {
        array.push(mockQuarter);
      } else if (field === mockYear) {
        array.push(mockYear);
      }
      return array;
    });

    const period = new Quarter(mockQuarter, mockYear);
    const result = dependencies.computePeriodDependencies(period);

    expect(result).toEqual(expect.arrayContaining([mockQuarter, mockYear]));
  });

  it('should compute dependencies for Month', () => {
    const computeFieldDependenciesSpy = jest.spyOn(dependencies, 'computeFieldDependencies');
    computeFieldDependenciesSpy.mockImplementation((field, array) => {
      if (field === mockMonth) {
        array.push(mockMonth);
      } else if (field === mockYear) {
        array.push(mockYear);
      }
      return array;
    });

    const period = new Month(mockMonth, mockYear);
    const result = dependencies.computePeriodDependencies(period);

    expect(result).toEqual(expect.arrayContaining([mockMonth, mockYear]));
  });

  it('should throw an error for unknown Period type', () => {
    class UnknownPeriod implements Period {
      readonly class: string;
    }

    const unknownPeriod = new UnknownPeriod();

    expect(() => dependencies.computePeriodDependencies(unknownPeriod)).toThrow(
            "Period with unknown type: class UnknownPeriod"
    );
  });

});
