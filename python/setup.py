from setuptools import setup, find_packages

setup(
    name = "zenoss.protocols",
    version = "0.1",
    packages = find_packages(),
    long_description=open('README.txt').read(),
    install_requires = [
        'setuptools',
        'httplib2',
        'zope.dottedname',
    ],
    include_package_data=True,
    package_data={
        'zenoss/protocols/twisted': ['zenoss/protocols/twisted/amqp0-9-1.xml'],
    },
    # in the zenoss namespace
    namespace_packages = ['zenoss'],
    # metadata for upload to PyPI
    author = "Zenoss",
    author_email = "support@zenoss.com",
    description = "Protobufs and AMQP client for integrating with Zenoss.",
    license = "GPLv2",
    keywords = "zenoss protocols",
    url = "http://www.zenoss.com/",
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'zenqdump = zenoss.protocols.scripts.zenqdump:main',
            'zenqinit = zenoss.protocols.scripts.zenqinit:main',
            'zenqpush = zenoss.protocols.scripts.zenqpush:main',
            'zenqload = zenoss.protocols.scripts.zenqload:main',
        ]
    }
)
