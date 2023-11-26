<?php

namespace App\EasyAdmin\Field\Configurator;

use App\EasyAdmin\Field\SfAssetFileField;
use EasyCorp\Bundle\EasyAdminBundle\Config\Crud;
use EasyCorp\Bundle\EasyAdminBundle\Context\AdminContext;
use EasyCorp\Bundle\EasyAdminBundle\Contracts\Field\FieldConfiguratorInterface;
use EasyCorp\Bundle\EasyAdminBundle\Dto\EntityDto;
use EasyCorp\Bundle\EasyAdminBundle\Dto\FieldDto;
use Symfony\Component\PropertyAccess\PropertyAccess;
use function Symfony\Component\String\u;

/**
 * @author Javier Eguiluz <javier.eguiluz@gmail.com>
 */
final class SfAssetFileConfigurator implements FieldConfiguratorInterface
{
    private string $projectDir;

    public function __construct(string $projectDir)
    {
        $this->projectDir = $projectDir;
    }

    public function supports(FieldDto $field, EntityDto $entityDto): bool
    {
        return SfAssetFileField::class === $field->getFieldFqcn();
    }

    public function configure(FieldDto $field, EntityDto $entityDto, AdminContext $context): void
    {
        $configuredBasePath = $field->getCustomOption(SfAssetFileField::OPTION_BASE_PATH);

        $formattedValue = \is_array($field->getValue())
            ? $this->getImagesPaths($field->getValue(), $configuredBasePath)
            : $this->getImagePath($field->getValue(), $configuredBasePath);
        $field->setFormattedValue($formattedValue);

        $field->setFormTypeOption('upload_filename', $field->getCustomOption(SfAssetFileField::OPTION_UPLOADED_FILE_NAME_PATTERN));

        // this check is needed to avoid displaying broken images when image properties are optional
        if (null === $formattedValue || '' === $formattedValue || (\is_array($formattedValue) && 0 === \count($formattedValue)) || $formattedValue === rtrim($configuredBasePath ?? '', '/')) {
            $field->setTemplateName('label/empty');
        }

        if (!\in_array($context->getCrud()->getCurrentPage(), [Crud::PAGE_EDIT, Crud::PAGE_NEW], true)) {
            return;
        }

        $relativeUploadDir = $field->getCustomOption(SfAssetFileField::OPTION_UPLOAD_DIR);
        if (null === $relativeUploadDir) {
            throw new \InvalidArgumentException(sprintf('The "%s" image field must define the directory where the images are uploaded using the setUploadDir() method.', $field->getProperty()));
        }
        $relativeUploadDir = u($relativeUploadDir)->trimStart(\DIRECTORY_SEPARATOR)->ensureEnd(\DIRECTORY_SEPARATOR)->toString();
        $isStreamWrapper = filter_var($relativeUploadDir, \FILTER_VALIDATE_URL);
        if ($isStreamWrapper) {
            $absoluteUploadDir = $relativeUploadDir;
        } else {
            $absoluteUploadDir = u($relativeUploadDir)->ensureStart($this->projectDir.\DIRECTORY_SEPARATOR)->toString();
        }
        $field->setFormTypeOption('upload_dir', $absoluteUploadDir);
        $accessor = PropertyAccess::createPropertyAccessor();
        $property = $field->getProperty();
        $field->setFormTypeOption('current_asset', $accessor->getValue($entityDto->getInstance(), $property));

    }

    private function getImagesPaths(?array $images, ?string $basePath): array
    {
        $imagesPaths = [];
        foreach ($images as $image) {
            $imagesPaths[] = $this->getImagePath($image, $basePath);
        }

        return $imagesPaths;
    }

    private function getImagePath(?string $imagePath, ?string $basePath): ?string
    {
        // add the base path only to images that are not absolute URLs (http or https) or protocol-relative URLs (//)
        if (null === $imagePath || 0 !== preg_match('/^(http[s]?|\/\/)/i', $imagePath)) {
            return $imagePath;
        }

        // remove project path from filepath
        $imagePath = str_replace($this->projectDir.\DIRECTORY_SEPARATOR.'public'.\DIRECTORY_SEPARATOR, '', $imagePath);

        return isset($basePath)
            ? rtrim($basePath, '/').'/'.ltrim($imagePath, '/')
            : '/'.ltrim($imagePath, '/');
    }
}
