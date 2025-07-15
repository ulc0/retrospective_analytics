# Copyright 2017 Palantir Technologies, Inc.
import logging
import os
import pkgutil
import site
import sys
import types
from collections.abc import Iterable
from importlib import reload
from typing import Any, Dict, Generator, List, Set, Union

import pkg_resources

from transforms import _errors, _utils

from . import _lightweight, _transform

log = logging.getLogger(__name__)


class Pipeline(object):
    """An object for grouping a collection of :class:`Transform` objects."""

    def __init__(self):
        self._transforms: List[_transform.Transform] = []
        self._compute_functions: Dict[str, _transform.Transform] = {}
        self._outputs: Dict[str, _transform.Transform] = {}
        self._container_transforms_configurations: List[
            _lightweight.ContainerTransformsConfiguration
        ] = []

    @property
    def container_transforms_configurations(
        self,
    ) -> List[_lightweight.ContainerTransformsConfiguration]:
        return self._container_transforms_configurations

    @property
    def transforms(self) -> List[_transform.Transform]:
        """List[Transform]: The list of transforms registered to the pipeline."""
        return self._transforms

    def add_transforms(
        self,
        *transforms: Union[
            _transform.Transform, _lightweight.ContainerTransformsConfiguration
        ],
    ) -> None:
        """Register the given :class:`Transform` objects to the `Pipeline` instance.

        Args:
            *transforms (Transform): The transforms to register.

        Raises:
            :exc:`~exceptions.ValueError`: if multiple :class:`Transform` objects write
                to the same :class:`Output` alias.
            :exc:`~exceptions.ValueError`: if multiple :class:`Transform` objects have same compute function reference.
        """
        for transform in transforms:
            if isinstance(transform, _lightweight.ContainerTransformsConfiguration):
                self._container_transforms_configurations.append(transform)

        spark_transforms = [
            transform
            for transform in transforms
            if not isinstance(transform, _lightweight.ContainerTransformsConfiguration)
        ]  # regular transforms that are not decorated with @lightweight

        for transform in spark_transforms:
            if not isinstance(transform, _transform.Transform):
                raise _errors.TransformTypeError(
                    f"Expected arguments to be of type {_transform.Transform}"
                )

        for transform in spark_transforms:
            self._check_transform_outputs(transform)

            # Register if bound
            if transform._bound_transform:
                # pylint: disable=expression-not-assigned
                self._transforms.append(transform)
            else:
                log.info(
                    "Found unbound transform with reference '%s': %s",
                    transform.reference,
                    transform.filename,
                )

            # Register as compute function
            if (
                transform.reference in self._compute_functions
                and self._compute_functions[transform.reference] != transform
            ):
                raise _errors.TransformValueError(
                    f"Multiple transforms have same reference {transform.reference!r}: "
                    f"{self._compute_functions[transform.reference].filename} and {transform.filename}. If transforms "
                    "are unbound and defined in same file, they can not have same function name."
                )
            self._compute_functions[transform.reference] = transform

    def _check_transform_outputs(self, transform: _transform.Transform) -> None:
        # Make sure same output isn't used by multiple transforms
        for output in transform.outputs.values():
            if (
                output.alias in self._outputs
                and self._outputs[output.alias] != transform
            ):
                raise _errors.TransformValueError(
                    f"Multiple transforms write to the alias {output.alias}: {self._outputs[output.alias].filename} "
                    f"and {transform.filename}"
                )

            self._outputs[output.alias] = transform

    def discover_transforms(self, *modules: types.ModuleType) -> None:
        """Recursively find and import modules registering every module-level transform.

        This method recursively finds and imports modules starting at the given module's `__path__`.
        Each module found is imported and any attribute that is an instance of :class:`~transforms.api.Transform`
        (as constructed by the transforms decorators) will be registered to the pipeline.

        >>> import myproject
        >>> p = Pipeline()
        >>> p.discover_transforms(myproject)

        Args:
            *modules (module): The modules to start searching from.

        Note:
            Each module found is imported. Try to avoid executing code at the module-level.
        """
        for mod in modules:
            if not isinstance(mod, types.ModuleType):
                raise _errors.TransformTypeError(
                    f"Expected arguments to be of type {types.ModuleType}"
                )

        non_packages: Set[str] = set()
        mods = set(modules)
        for mod in modules:
            if hasattr(mod, "__path__"):  # It is a package
                for loader, module_name, _pkg in pkgutil.walk_packages(
                    path=mod.__path__, prefix=mod.__name__ + "."
                ):
                    new_module = load_module(loader, module_name)
                    mods.add(new_module)

                    # pkgutil isn't adding submodules to their parents' __dict__ attributes,
                    # which is causing imports inside the submodules to fail, see:
                    # https://stackoverflow.com/questions/46456309/why-does-pkgutil-fail-to-import-submodules
                    # Fixed by manually adding the submodules to their parent's __dict__.
                    head, _, tail = module_name.rpartition(".")
                    if head:
                        setattr(sys.modules[head], tail, new_module)

                # For Python versions lower than 3.3 we also check that there aren't any non-package directories
                # under the given packages. From version 3.3 directories have implicitly assigned package,
                # see: https://www.python.org/dev/peps/pep-0420/.
                # If there are, it's almost certainly user error and we won't find their transforms!
                # `__pycache__` is an exception, see: https://www.python.org/dev/peps/pep-3147/.
                if _utils.python_version_at_least(3, 3):
                    continue
                for path in mod.__path__:
                    for dirpath, _dirnames, filenames in os.walk(path):
                        if (
                            "__init__.py" not in filenames
                            and os.path.basename(dirpath) != "__pycache__"
                        ):
                            non_packages.add(dirpath)

        if non_packages:
            raise _errors.TransformValueError(
                "Non-package directories were found while running transform discovery. "
                f"Please create an __init__.py file in each of {non_packages}"
            )

        log.info("Searching for transforms in modules: %s", mods)

        for mod in mods:
            module_tforms = list(_find_transforms(mod))
            log.info("Found transforms in module %s: %s", mod.__name__, module_tforms)

            self.add_transforms(*module_tforms)

    def _get_transform_by_outputs(
        self, alias: str, *aliases: str
    ) -> _transform.Transform:
        """Return the :class:`Transform` object that writes to the given aliases.

        Args:
            alias (str): An alias the :class:`Transform` must write to.
            *aliases (str): Other aliases the :class:`Transform` must write to.

        Returns:
            Transform: The transform object that writes to exactly the given aliases.

        Raises:
            :exc:`~exceptions.KeyError`: If no such transform exists.
            :exc:`~exceptions.ValueError`: If a transform exists that writes to one of the
                given aliases, but doesn't write to exactly all of them.
        """
        transform = self._outputs[alias]
        tform_aliases = [o.alias for o in transform.outputs.values()]

        all_aliases = [alias] + list(aliases)
        if set(tform_aliases) != set(all_aliases):
            raise _errors.TransformValueError(
                f"Expected transform {transform} to write to {all_aliases}, but it writes to {tform_aliases}"
            )
        return transform

    def _get_transform_by_reference(self, reference: str) -> _transform.Transform:
        """Return the :class:`Transform` object with given compute function reference.

        Args:
            reference (str): A reference to search for.

        Returns:
            Transform: The transform object with the same reference.

        Raises:
            :exc:`~exceptions.KeyError`: If no such transform exists.
        """
        return self._compute_functions[reference]

    @classmethod
    def _from_entry_point(cls, name: str = "root"):
        """Load a `Pipeline` from a registered entry point.

        Args:
            name (str): The name of the registered `Pipeline`.

        To register a `Pipeline` named `root`, add the following to your ``setup.py``::

            setup(
                ...
                entry_points={
                    'transforms.pipelines': [
                        'root = mypackage.mymodule:my_pipeline'
                    ]
                }
            )
        """

        _hack: Union[str, None] = None

        # see: http://bugs.python.org/issue30167
        _hack = str(
            sys.modules["__main__"].__loader__.__module__
        )  # pylint: disable=no-member
        sys.modules[
            "__main__"
        ].__loader__.__module__ += "_"  # pylint: disable=no-member

        reload(site)

        # restore original value
        sys.modules[
            "__main__"
        ].__loader__.__module__ = _hack  # pylint: disable=no-member

        ws = pkg_resources.WorkingSet()
        pipelines = {e.name: e for e in ws.iter_entry_points("transforms.pipelines")}
        if name not in pipelines:
            raise _errors.EntryPointError(
                f"Key {name} was not found, please check your repo's meta.yaml and setup.py files"
            )
        return pipelines[name].load()


def _find_transforms(
    module: types.ModuleType,
) -> Generator[
    Union[_lightweight.ContainerTransformsConfiguration, _transform.Transform],
    None,
    None,
]:
    for attr in dir(module):
        _value: Any = getattr(module, attr)
        value: List[Any] = list(_value) if isinstance(_value, Iterable) else [_value]

        for item in value:
            if hasattr(item, "_is_incremental") and item._is_incremental is True:
                raise _errors.TransformTypeError(
                    f"Uninvoked incremental decorator found in {module.__file__}. "
                    "Are you trying to use @incremental instead of @incremental()?"
                )

        if all(
            isinstance(
                item,
                (_lightweight.ContainerTransformsConfiguration, _transform.Transform),
            )
            for item in value
        ):
            for item in value:
                yield item


def load_module(loader, module_name: str):
    try:
        return loader.find_module(module_name).load_module(module_name)
    except TypeError as e:
        _raise_if_overridden_transform_function(e, module_name)
        raise e


def _raise_if_overridden_transform_function(e: TypeError, module_name: str) -> None:
    if str(e).startswith("transform() got an unexpected keyword argument"):
        raise _errors.OverloadedTransformFunction(
            f"transform() decorator was called with invalid argument in '{module_name}'.\n"
            "Check that @transform is referring to 'transforms.api.transform' and not "
            "'pyspark.sql.functions.transform'.\n"
            "Are you importing 'from pyspark.sql.functions import *'?"
        ) from e
